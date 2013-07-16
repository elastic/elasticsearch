package org.elasticsearch.test.integration.search.functionscore;

import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.index.query.functionscore.DecayFunction;
import org.elasticsearch.index.query.functionscore.DecayFunctionParser;

public class CustomDistanceScoreParser extends DecayFunctionParser {

    public static final String[] NAMES = {"linear_mult"};

    @Override
    public String[] getNames() {
        return NAMES;
    }

    static final DecayFunction distanceFunction = new LinearMultScoreFunction();;

    @Override
    public DecayFunction getDecayFunction() {
        return distanceFunction;
    }

    static class LinearMultScoreFunction implements DecayFunction {
        LinearMultScoreFunction() {
        }

        @Override
        public double evaluate(double value, double scale) {
            return Math.abs(value);
        }

        @Override
        public Explanation explainFunction(String distanceString, double distanceVal, double scale) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setDescription("" + distanceVal);
            return ce;
        }

        @Override
        public double processScale(double userGivenScale, double userGivenValue) {
            return userGivenScale;
        }
    }
}
