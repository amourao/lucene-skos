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
import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

import at.ac.univie.mminf.luceneSKOS.analysis.tokenattributes.SKOSTypeAttribute.SKOSType;
import at.ac.univie.mminf.luceneSKOS.skos.SKOSEngine;
import at.ac.univie.mminf.luceneSKOS.skos.SKOSEngineFactory;
import at.ac.univie.mminf.luceneSKOS.skos.impl.MeSHEngineImpl;

/**
 * An analyzer for expanding fields that contain either (i) URI references to
 * SKOS concepts OR (ii) SKOS concept prefLabels as values.
 */
public class MeSHAnalyzer extends StopwordAnalyzerBase {
  
  /** The supported expansion types */
  public enum ExpansionType {
    URI, LABEL
  }
  
  /** Default expansion type */
  public static final ExpansionType DEFAULT_EXPANSION_TYPE = ExpansionType.LABEL;
  
  protected ExpansionType expansionType = DEFAULT_EXPANSION_TYPE;
  
  /** Default skos types to expand to */
  public static final SKOSType[] DEFAULT_SKOS_TYPES = new SKOSType[] {
      SKOSType.PREF, SKOSType.ALT, SKOSType.BROADER,
      SKOSType.BROADERTRANSITIVE, SKOSType.NARROWER,
      SKOSType.NARROWERTRANSITIVE};
  
  public static final SKOSType[] DEFAULT_MESH_TYPES = new SKOSType[] {
		SKOSType.PREF, SKOSType.ALT, SKOSType.BROADER1, SKOSType.BROADER2,
		SKOSType.BROADER3, SKOSType.BROADER4, SKOSType.BROADER5,
		SKOSType.BROADER6, SKOSType.BROADER7, SKOSType.BROADER8,
		SKOSType.BROADER9, SKOSType.BROADER10, SKOSType.BROADER11,
		SKOSType.BROADER12, SKOSType.NARROWER1, SKOSType.NARROWER2,
		SKOSType.NARROWER3, SKOSType.NARROWER4, SKOSType.NARROWER5,
		SKOSType.NARROWER6, SKOSType.NARROWER7, SKOSType.NARROWER8,
		SKOSType.NARROWER9, SKOSType.NARROWER10, SKOSType.NARROWER11,
		SKOSType.NARROWER12 };
  
  
  public static final SKOSType[] ALT_MESH_TYPES = new SKOSType[] {
		SKOSType.PREF, SKOSType.ALT, SKOSType.BROADER1, SKOSType.NARROWER1, SKOSType.NARROWER2};


  
  private SKOSType[] types = ALT_MESH_TYPES;
  
  /** A SKOS Engine instance */
  protected MeSHEngineImpl skosEngine;
  
  /** The size of the buffer used for multi-term prediction */
  protected int bufferSize = SKOSLabelFilter.DEFAULT_BUFFER_SIZE;
  
  /** Default maximum allowed token length */
  public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;
  
  private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;
  
  /**
   * An unmodifiable set containing some common English words that are usually
   * not useful for searching.
   */
  public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
  
  public MeSHAnalyzer(Version matchVersion, CharArraySet stopWords,
		  MeSHEngineImpl skosEngine, ExpansionType expansionType) {
    super(matchVersion, stopWords);
    this.skosEngine = skosEngine;
    this.expansionType = expansionType;
  }
  
  public MeSHAnalyzer(Version matchVersion, MeSHEngineImpl skosEngine,
      ExpansionType expansionType) {
    this(matchVersion, STOP_WORDS_SET, skosEngine, expansionType);
  }
  
  public MeSHAnalyzer(Version matchVersion, Reader stopwords,
		  MeSHEngineImpl skosEngine, ExpansionType expansionType) throws IOException {
    this(matchVersion, loadStopwordSet(stopwords, matchVersion), skosEngine,
        expansionType);
  }
  
  public MeSHAnalyzer(Version matchVersion, CharArraySet stopWords,
      String skosFile, ExpansionType expansionType, int bufferSize,
      String... languages) throws IOException {
    super(matchVersion, stopWords);
    this.skosEngine = new MeSHEngineImpl(matchVersion, skosFile, languages);
    this.expansionType = expansionType;
    this.bufferSize = bufferSize;
  }
  
  public MeSHAnalyzer(Version matchVersion, String skosFile,
      ExpansionType expansionType, int bufferSize, String... languages)
      throws IOException {
    this(matchVersion, STOP_WORDS_SET, skosFile, expansionType, bufferSize,
        languages);
  }
  
  public MeSHAnalyzer(Version matchVersion, String skosFile,
      ExpansionType expansionType, int bufferSize) throws IOException {
    this(matchVersion, skosFile, expansionType, bufferSize, (String[]) null);
  }
  
  public MeSHAnalyzer(Version matchVersion, String skosFile,
      ExpansionType expansionType) throws IOException {
    this(matchVersion, skosFile, expansionType,
        SKOSLabelFilter.DEFAULT_BUFFER_SIZE);
  }
  
  public MeSHAnalyzer(Version matchVersion, Reader stopwords, String skosFile,
      ExpansionType expansionType, int bufferSize, String... languages)
      throws IOException {
    this(matchVersion, loadStopwordSet(stopwords, matchVersion), skosFile,
        expansionType, bufferSize, languages);
  }
  
  public SKOSType[] getTypes() {
    return types;
  }
  
  public void setTypes(SKOSType... types) {
    this.types = types;
  }
  
  /**
   * Set maximum allowed token length. If a token is seen that exceeds this
   * length then it is discarded. This setting only takes effect the next time
   * tokenStream or tokenStream is called.
   */
  public void setMaxTokenLength(int length) {
    maxTokenLength = length;
  }
  
  /**
   * @see #setMaxTokenLength
   */
  public int getMaxTokenLength() {
    return maxTokenLength;
  }
  
  @Override
  protected TokenStreamComponents createComponents(String fileName,
      Reader reader) {
    if (expansionType.equals(ExpansionType.URI)) {
      final KeywordTokenizer src = new KeywordTokenizer(reader);
      TokenStream tok = new MeSHURIFilter(src, skosEngine,
          new StandardAnalyzer(matchVersion), types);
      tok = new LowerCaseFilter(matchVersion, tok);
      return new TokenStreamComponents(src, tok);
    } else {
      final StandardTokenizer src = new StandardTokenizer(matchVersion, reader);
      src.setMaxTokenLength(maxTokenLength);
      TokenStream tok = new StandardFilter(matchVersion, src);
      // prior to this we get the classic behavior, standardfilter does it for
      // us.
      tok = new MeSHLabelFilter(tok, skosEngine, new StandardAnalyzer(
          matchVersion), bufferSize, types);
      tok = new LowerCaseFilter(matchVersion, tok);
      tok = new StopFilter(matchVersion, tok, stopwords);
      tok = new RemoveDuplicatesTokenFilter(tok);
      return new TokenStreamComponents(src, tok) {
        @Override
        protected void setReader(final Reader reader) throws IOException {
          src.setMaxTokenLength(maxTokenLength);
          super.setReader(reader);
        }
      };
    }
  }
}

