package at.ac.univie.mminf.luceneSKOS.queryparser.flexible.standard;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorPipeline;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.processors.AnalyzerQueryNodeProcessor;

import at.ac.univie.mminf.luceneSKOS.analysis.tokenattributes.SKOSTypeAttribute.SKOSType;
import at.ac.univie.mminf.luceneSKOS.queryparser.flexible.standard.processors.MeSHQueryNodeProcessor;
import at.ac.univie.mminf.luceneSKOS.queryparser.flexible.standard.processors.SKOSQueryNodeProcessor;

public class MeSHStandadQueryParser extends StandardQueryParser {

	private Map<SKOSType, Float> boosts = new HashMap<SKOSType, Float>() {
		private static final long serialVersionUID = 1L;
		{
			put(SKOSType.PREF, 0f);
			put(SKOSType.ALT, 0f);
			put(SKOSType.HIDDEN, 0f);
			put(SKOSType.BROADER, 0f);
			put(SKOSType.NARROWER, 0f);
			put(SKOSType.BROADERTRANSITIVE, 0f);
			put(SKOSType.NARROWERTRANSITIVE, 0f);
			put(SKOSType.RELATED, 0f);

			for (int i = 1; i <= 12; i++) {

				SKOSType b = SKOSType.valueOf(SKOSType.BROADER.toString() + i);
				SKOSType n = SKOSType.valueOf(SKOSType.NARROWER.toString() + i);

				put(b, 0f);
				put(n, 0f);
			}

		}
	};

	public enum scoringFunctionTypes {
		EXP,EXP2, LINEAR, NO_DECAY
	}

	public MeSHStandadQueryParser(Analyzer analyzer) {
		super();
		QueryNodeProcessorPipeline qnpp = ((QueryNodeProcessorPipeline) getQueryNodeProcessor());

		int i = 0;
		for (i = 0; i < qnpp.size(); i++) {
			if (qnpp.get(i) instanceof AnalyzerQueryNodeProcessor) {
				break;
			}
		}

		MeSHQueryNodeProcessor qnp = new MeSHQueryNodeProcessor(analyzer);
		qnp.setBoosts(boosts);
		qnpp.add(i, qnp);

		// Set boost map

	}

	public void setBoosts(Map<SKOSType, Float> boosts) {
		this.boosts = boosts;
	}

	public Map<SKOSType, Float> getBoosts() {
		return boosts;
	}

	public void setBoost(SKOSType skosType, float boost) {
		boosts.put(skosType, boost);
	}

	public void genBoosts(SKOSType t, double minVal, double maxVal,
			int maxLevel, scoringFunctionTypes type) {

		for (int i = 0; i < maxLevel; i++) {

			SKOSType b = SKOSType.valueOf(t.toString() + (i + 1));
			double score = ((maxLevel - i) / (double) maxLevel);

			if (type == scoringFunctionTypes.EXP) {
				score = 1 / Math.pow((score + 1), 1);
				score = (score * (maxVal - minVal)) + minVal;
			} else if (type == scoringFunctionTypes.EXP2) {
				score = 1 / Math.pow((score + 1), 2);
				score = (score * (maxVal - minVal)) + minVal;
			} else if (type == scoringFunctionTypes.LINEAR) {
				score = (score * (maxVal - minVal)) + minVal;
			} else if (type == scoringFunctionTypes.NO_DECAY) {
				score = maxVal;
			}

			boosts.put(b, (float) score);
		}
	}

	public float getBoost(String type) {
		if (boosts == null) {
			return 1;
		}

		Float boost = boosts.get(type);

		if (boost != null) {
			return boost;
		}

		return 1;
	}

}
