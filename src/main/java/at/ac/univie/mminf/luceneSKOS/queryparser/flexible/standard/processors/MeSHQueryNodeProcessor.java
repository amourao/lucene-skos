package at.ac.univie.mminf.luceneSKOS.queryparser.flexible.standard.processors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.BoostQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NoTokenFoundQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TextableQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.StandardBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import at.ac.univie.mminf.luceneSKOS.analysis.tokenattributes.SKOSTypeAttribute;
import at.ac.univie.mminf.luceneSKOS.analysis.tokenattributes.SKOSTypeAttribute.SKOSType;

public class MeSHQueryNodeProcessor extends QueryNodeProcessorImpl {

	  private Analyzer analyzer;

	  private boolean positionIncrementsEnabled;

	  private Map<SKOSType,Float> boosts;

	  public MeSHQueryNodeProcessor(Analyzer analyzer) {
	    this.analyzer = analyzer;
	  }

	  @Override
	  public QueryNode process(QueryNode queryTree) throws QueryNodeException {
	    if (analyzer != null) {
	      this.positionIncrementsEnabled = false;
	      Boolean positionIncrementsEnabled = getQueryConfigHandler().get(ConfigurationKeys.ENABLE_POSITION_INCREMENTS);

	      if (positionIncrementsEnabled != null) {
	          this.positionIncrementsEnabled = positionIncrementsEnabled;
	      }

	      if (this.analyzer != null) {
	        return super.process(queryTree);
	      }

	    }

	    return queryTree;

	  }

	  @Override
	  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

	    if (node instanceof TextableQueryNode
	        && !(node instanceof WildcardQueryNode)
	        && !(node instanceof FuzzyQueryNode)
	        && !(node instanceof RegexpQueryNode)
	        && !(node.getParent() instanceof RangeQueryNode)) {

	      FieldQueryNode fieldNode = ((FieldQueryNode) node);
	      String text = fieldNode.getTextAsString();
	      String field = fieldNode.getFieldAsString();

	      TokenStream source;
	      try {
	        source = this.analyzer.tokenStream(field, text);
	        source.reset();
	      } catch (IOException e1) {
	        throw new RuntimeException(e1);
	      }
	      CachingTokenFilter buffer = new CachingTokenFilter(source);

	      PositionIncrementAttribute posIncrAtt = null;
	      int numTokens = 0;
	      int positionCount = 0;
	      boolean severalTokensAtSamePosition = false;

	      if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
	        posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
	      }

	      try {

	        while (buffer.incrementToken()) {
	          numTokens++;
	          int positionIncrement = (posIncrAtt != null) ? posIncrAtt
	              .getPositionIncrement() : 1;
	          if (positionIncrement != 0) {
	            positionCount += positionIncrement;

	          } else {
	            severalTokensAtSamePosition = true;
	          }

	        }

	      } catch (IOException e) {
	        // ignore
	      }

	      try {
	        // rewind the buffer stream
	        buffer.reset();

	        // close original stream - all tokens buffered
	        source.close();
	      } catch (IOException e) {
	        // ignore
	      }

	      if (!buffer.hasAttribute(CharTermAttribute.class)) {
	        return new NoTokenFoundQueryNode();
	      }

	      CharTermAttribute termAtt = buffer.getAttribute(CharTermAttribute.class);

	      if (numTokens == 0) {
	        return new NoTokenFoundQueryNode();

	      } else if (numTokens == 1) {
	        String term = null;
	        try {
	          boolean hasNext;
	          hasNext = buffer.incrementToken();
	          assert hasNext == true;
	          term = termAtt.toString();

	        } catch (IOException e) {
	          // safe to ignore, because we know the number of tokens
	        }

	        fieldNode.setText(term);

	        return fieldNode;

	      } else if (severalTokensAtSamePosition || !(node instanceof QuotedFieldQueryNode)) {
	        if (positionCount == 1 || !(node instanceof QuotedFieldQueryNode)) {
	          // no phrase query:
	          LinkedList<QueryNode> children = new LinkedList<QueryNode>();

	          for (int i = 0; i < numTokens; i++) {
	            String term = null;
	            try {
	              boolean hasNext = buffer.incrementToken();
	              assert hasNext == true;
	              term = termAtt.toString();

	            } catch (IOException e) {
	              // safe to ignore, because we know the number of tokens
	            }
	            
	            if (buffer.hasAttribute(SKOSTypeAttribute.class) && boosts != null) {

	              SKOSTypeAttribute skosAttr = buffer.getAttribute(SKOSTypeAttribute.class);

	              
	              MultiPhraseQueryNode mpq = new MultiPhraseQueryNode();
	              //children.add(new BoostQueryNode(new FieldQueryNode(field, "\""+ term +"\"", -1, -1), getBoost(skosAttr.getSkosType())));
	              children.add(new BoostQueryNode(new FieldQueryNode(field, term, -1, -1), getBoost(skosAttr.getSkosType())));

	            } else {

	              children.add(new FieldQueryNode(field, term, -1, -1));

	            }

	          }
	          return new GroupQueryNode(
	            new StandardBooleanQueryNode(children, positionCount==1));
	        } else {
	          // phrase query:
	          MultiPhraseQueryNode mpq = new MultiPhraseQueryNode();

	          List<FieldQueryNode> multiTerms = new ArrayList<FieldQueryNode>();
	          int position = -1;
	          int i = 0;
	          int termGroupCount = 0;
	          for (; i < numTokens; i++) {
	            String term = null;
	            int positionIncrement = 1;
	            try {
	              boolean hasNext = buffer.incrementToken();
	              assert hasNext == true;
	              term = termAtt.toString();
	              if (posIncrAtt != null) {
	                positionIncrement = posIncrAtt.getPositionIncrement();
	              }

	            } catch (IOException e) {
	              // safe to ignore, because we know the number of tokens
	            }

	            if (positionIncrement > 0 && multiTerms.size() > 0) {

	              for (FieldQueryNode termNode : multiTerms) {

	                if (this.positionIncrementsEnabled) {
	                  termNode.setPositionIncrement(position);
	                } else {
	                  termNode.setPositionIncrement(termGroupCount);
	                }

	                mpq.add(termNode);

	              }

	              // Only increment once for each "group" of
	              // terms that were in the same position:
	              termGroupCount++;

	              multiTerms.clear();

	            }

	            position += positionIncrement;
	            multiTerms.add(new FieldQueryNode(field, term, -1, -1));

	          }

	          for (FieldQueryNode termNode : multiTerms) {

	            if (this.positionIncrementsEnabled) {
	              termNode.setPositionIncrement(position);

	            } else {
	              termNode.setPositionIncrement(termGroupCount);
	            }

	            mpq.add(termNode);

	          }

	          return mpq;

	        }

	      } else {

	        TokenizedPhraseQueryNode pq = new TokenizedPhraseQueryNode();

	        int position = -1;

	        for (int i = 0; i < numTokens; i++) {
	          String term = null;
	          int positionIncrement = 1;

	          try {
	            boolean hasNext = buffer.incrementToken();
	            assert hasNext == true;
	            term = termAtt.toString();

	            if (posIncrAtt != null) {
	              positionIncrement = posIncrAtt.getPositionIncrement();
	            }

	          } catch (IOException e) {
	            // safe to ignore, because we know the number of tokens
	          }

	          FieldQueryNode newFieldNode = new FieldQueryNode(field, term, -1, -1);

	          if (this.positionIncrementsEnabled) {
	            position += positionIncrement;
	            newFieldNode.setPositionIncrement(position);

	          } else {
	            newFieldNode.setPositionIncrement(i);
	          }

	          pq.add(newFieldNode);

	        }

	        return pq;

	      }

	    }

	    return node;

	  }

	  public void setBoosts(Map<SKOSType,Float> boosts) {
	    this.boosts = boosts;
	  }

	  public Map<SKOSType, Float> getBoosts() {
	    return boosts;
	  }
	  
	  public void setBoost(SKOSType skosType, float boost) {
	    boosts.put(skosType, boost);
	  }

	  public float getBoost(SKOSType skosType) {
	    if (boosts == null) {
	      return 1;
	    }
	    
	    Float boost = boosts.get(skosType);
	    
	    if (boost != null) {
	      return boost;
	    }
	    
	    return 1;
	  }

	  @Override
	  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

	    return node;

	  }

	  @Override
	  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
	      throws QueryNodeException {

	    return children;

	  }

	}

