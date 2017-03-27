package at.ac.univie.mminf.luceneSKOS.analysis;

/**
 * Copyright 2010 Bernhard Haslhofer 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.AttributeSource.State;

import at.ac.univie.mminf.luceneSKOS.analysis.tokenattributes.SKOSTypeAttribute;
import at.ac.univie.mminf.luceneSKOS.analysis.tokenattributes.SKOSTypeAttribute.SKOSType;
import at.ac.univie.mminf.luceneSKOS.skos.SKOSEngine;
import at.ac.univie.mminf.luceneSKOS.skos.impl.MeSHEngineImpl;

/**
 * A SKOS-specific TokenFilter implementation
 */
public class SNOMEDFilter extends TokenFilter {

	/* a stack holding the expanded terms for a token */
	protected Stack<ExpandedTerm> termStack;

	/* an engine delivering SKOS concepts */
	protected SKOSEngine engine;

	/* the skos types to expand to */
	protected Set<SKOSType> types;

	/* provides access to the the term attributes */
	protected AttributeSource.State current;

	/* the term text (propagated to the index) */
	protected final CharTermAttribute termAtt;

	/* the token position relative to the previous token (propagated) */
	protected final PositionIncrementAttribute posIncrAtt;

	/* the binary payload attached to the indexed term (propagated to the index) */
	protected final PayloadAttribute payloadAtt;

	/* the SKOS-specific attribute attached to a term */
	protected final SKOSTypeAttribute skosAtt;

	/* the analyzer to use when parsing */
	protected final Analyzer analyzer;

	/**
	 * Constructor
	 * 
	 * @param input
	 *            the TokenStream
	 * @param skosEngine
	 *            the engine delivering skos concepts
	 * @param type
	 *            the skos types to expand to
	 */
	public SNOMEDFilter(TokenStream input, SKOSEngine skosEngine,
			Analyzer analyzer, SKOSType... types) {
		super(input);
		termStack = new Stack<ExpandedTerm>();
		this.engine = skosEngine;
		this.analyzer = analyzer;

		if (types != null && types.length > 0) {
			this.types = new TreeSet<SKOSType>(Arrays.asList(types));
		} else {
			this.types = new TreeSet<SKOSType>(Arrays.asList(new SKOSType[] {
					SKOSType.PREF, SKOSType.ALT }));
		}

		this.termAtt = addAttribute(CharTermAttribute.class);
		this.posIncrAtt = addAttribute(PositionIncrementAttribute.class);
		this.payloadAtt = addAttribute(PayloadAttribute.class);
		this.skosAtt = addAttribute(SKOSTypeAttribute.class);
	}

	public static final int DEFAULT_BUFFER_SIZE = 1;

	/* the size of the buffer used for multi-term prediction */
	private int bufferSize = DEFAULT_BUFFER_SIZE;

	/* a list serving as token buffer between consumed and consuming stream */
	private Queue<State> buffer = new LinkedList<State>();

	/**
	 * Constructor for multi-term expansion support. Takes an input token
	 * stream, the SKOS engine, and an integer indicating the maximum token
	 * length of the preferred labels in the SKOS vocabulary.
	 * 
	 * @param input
	 *            the consumed token stream
	 * @param skosEngine
	 *            the skos expansion engine
	 * @param bufferSize
	 *            the length of the longest pref-label to consider (needed for
	 *            mult-term expansion)
	 * @param types
	 *            the skos types to expand to
	 */
	public SNOMEDFilter(TokenStream input, SKOSEngine skosEngine,
			Analyzer analyzer, int bufferSize, SKOSType... types) {
		this(input, skosEngine, analyzer, types);
		this.bufferSize = bufferSize;
	}

	/**
	 * Advances the stream to the next token
	 */
	@Override
	public boolean incrementToken() throws IOException {
		/* there are expanded terms for the given token */
		if (termStack.size() > 0) {
			processTermOnStack();
			return true;
		}

		while (buffer.size() < bufferSize && input.incrementToken()) {
			buffer.add(input.captureState());

		}

		if (buffer.isEmpty()) {
			return false;
		}

		restoreState(buffer.peek());

		/* check whether there are expanded terms for a given token */
		if (addAliasesToStack()) {
			/* if yes, capture the state of all attributes */
			current = captureState();
		}

		buffer.remove();

		return true;
	}

	private boolean addAliasesToStack() throws IOException {
		for (int i = buffer.size(); i > 0; i--) {
			String inputTokens = bufferToString(i);

			if (addTermsToStack(inputTokens)) {
				break;
			}

		}

		if (termStack.isEmpty()) {
			return false;
		}

		return true;
	}

	/**
	 * Converts the first x=noTokens states in the queue to a concatenated token
	 * string separated by white spaces
	 */
	private String bufferToString(int noTokens) {
		State entered = captureState();

		State[] bufferedStates = buffer.toArray(new State[buffer.size()]);

		StringBuilder builder = new StringBuilder();
		builder.append(termAtt.toString());
		restoreState(bufferedStates[0]);
		for (int i = 1; i < noTokens; i++) {
			restoreState(bufferedStates[i]);
			builder.append(" " + termAtt.toString());
		}

		restoreState(entered);

		return builder.toString();
	}

	/**
	 * Assumes that the given term is a textual token
	 * 
	 */
	public boolean addTermsToStack(String term) throws IOException {
		try {
			String[] conceptURIs = engine.getConcepts(term);

			for (String conceptURI : conceptURIs) {
				if (types.contains(SKOSType.PREF)) {
					String[] prefLabels = engine.getPrefLabels(conceptURI);
					pushLabelsToStack(prefLabels, SKOSType.PREF);
				}
				if (types.contains(SKOSType.ALT)) {
					String[] altLabels = engine.getAltLabels(conceptURI);
					pushLabelsToStack(altLabels, SKOSType.ALT);
				}
				if (types.contains(SKOSType.HIDDEN)) {
					String[] hiddenLabels = engine.getHiddenLabels(conceptURI);
					pushLabelsToStack(hiddenLabels, SKOSType.HIDDEN);
				}
				if (types.contains(SKOSType.BROADER)) {
					String[] broaderLabels = engine
							.getBroaderLabels(conceptURI);
					pushLabelsToStack(broaderLabels, SKOSType.BROADER);
				}
				if (types.contains(SKOSType.BROADERTRANSITIVE)) {
					String[] broaderTransitiveLabels = engine
							.getBroaderTransitiveLabels(conceptURI);
					pushLabelsToStack(broaderTransitiveLabels,
							SKOSType.BROADERTRANSITIVE);
				}
				if (types.contains(SKOSType.NARROWER)) {
					String[] narrowerLabels = engine
							.getNarrowerLabels(conceptURI);
					pushLabelsToStack(narrowerLabels, SKOSType.NARROWER);
				}
				if (types.contains(SKOSType.NARROWERTRANSITIVE)) {
					String[] narrowerTransitiveLabels = engine
							.getNarrowerTransitiveLabels(conceptURI);
					pushLabelsToStack(narrowerTransitiveLabels,
							SKOSType.NARROWERTRANSITIVE);
				}
			}
		} catch (Exception e) {
			System.err.println("Error when accessing SKOS Engine.\n"
					+ e.getMessage());
		}

		if (termStack.isEmpty()) {
			return false;
		}

		return true;
	}

	public int getBufferSize() {
		return this.bufferSize;
	}

	/**
	 * Replaces the current term (attributes) with term (attributes) from the
	 * stack
	 * 
	 * @throws IOException
	 */
	protected void processTermOnStack() throws IOException {
		ExpandedTerm expandedTerm = termStack.pop();

		String term = expandedTerm.getTerm();

		SKOSType termType = expandedTerm.getTermType();

		String sTerm = "";

		try {
			sTerm = analyze(analyzer, term, new CharsRef()).toString();
		} catch (IllegalArgumentException e) {
			// skip this term
			return;
		}

		/*
		 * copies the values of all attribute implementations from this state
		 * into the implementations of the target stream
		 */
		restoreState(current);

		/*
		 * Adds the expanded term to the term buffer
		 */
		termAtt.setEmpty().append(sTerm);

		/*
		 * set position increment to zero to put multiple terms into the same
		 * position
		 */
		posIncrAtt.setPositionIncrement(0);

		/*
		 * sets the type of the expanded term (pref, alt, broader, narrower,
		 * etc.)
		 */
		skosAtt.setSkosType(termType);

		/*
		 * converts the SKOS Attribute to a payload, which is propagated to the
		 * index
		 */
		byte[] bytes = PayloadHelper.encodeInt(skosAtt.getSkosType().ordinal());
		payloadAtt.setPayload(new BytesRef(bytes));
	}

	/* Snipped from Solr's SynonymMap */
	public static CharsRef analyze(Analyzer analyzer, String text,
			CharsRef reuse) throws IOException {
		TokenStream ts = analyzer.tokenStream("", new StringReader(text));
		CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
		// PositionIncrementAttribute posIncAtt =
		// ts.addAttribute(PositionIncrementAttribute.class);
		boolean phraseTerm = false;
		ts.reset();
		reuse.length = 0;
		while (ts.incrementToken()) {
			// System.out.println(text + " | " + termAtt.toString());
			int length = termAtt.length();
			if (length == 0) {
				throw new IllegalArgumentException("term: " + text
						+ " analyzed to a zero-length token");
			}
			// if (posIncAtt.getPositionIncrement() != 1) {
			// throw new IllegalArgumentException("term: " + text +
			// " analyzed to a token with posinc != 1");
			// }
			reuse.grow(reuse.length + length + 1); /*
													 * current + word +
													 * separator
													 */
			int end = reuse.offset + reuse.length;
			if (reuse.length > 0) {
				reuse.chars[end++] = 32; // space
				reuse.length++;
				phraseTerm = true;
			}
			System.arraycopy(termAtt.buffer(), 0, reuse.chars, end, length);
			reuse.length += length;
		}
		ts.end();
		ts.close();
		if (reuse.length == 0) {
			throw new IllegalArgumentException("term: " + text
					+ " was completely eliminated by analyzer");
		}

		if (phraseTerm) {
			reuse.grow(reuse.length + 2); /* current + word + separator */
			reuse.length += 2;
			char next = reuse.chars[0];
			for (int i = 0; i < reuse.length - 2; i++) {
				char tmp = reuse.chars[i + 1];
				reuse.chars[i + 1] = next;
				next = tmp;
			}
			reuse.chars[0] = '\"';
			reuse.chars[reuse.length - 1] = '\"';
		}
		return reuse;
	}

	/**
	 * Pushes a given set of labels onto the stack
	 * 
	 * @param labels
	 * @param type
	 */
	protected void pushLabelsToStack(String[] labels, SKOSType type) {

		if (labels != null) {
			for (String label : labels) {
				termStack.push(new ExpandedTerm(label, type));
			}
		}

	}

	/**
	 * Helper class for capturing terms and term types
	 */
	protected static class ExpandedTerm {

		private final String term;

		private final SKOSType termType;

		protected ExpandedTerm(String term, SKOSType termType) {
			this.term = term;
			this.termType = termType;
		}

		protected String getTerm() {
			return this.term;
		}

		protected SKOSType getTermType() {
			return this.termType;
		}
	}
}
