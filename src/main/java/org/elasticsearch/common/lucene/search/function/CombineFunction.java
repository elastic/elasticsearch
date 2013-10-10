/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;

public enum CombineFunction {
    MULT {
        @Override
        public float combine(double queryBoost, double queryScore, double funcScore, double maxBoost) {
            return toFloat(queryBoost * queryScore * Math.min(funcScore, maxBoost));
        }

        @Override
        public String getName() {
            return "multiply";
        }

        @Override
        public ComplexExplanation explain(float queryBoost, Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            float score = queryBoost * Math.min(funcExpl.getValue(), maxBoost) * queryExpl.getValue();
            ComplexExplanation res = new ComplexExplanation(true, score, "function score, product of:");
            res.addDetail(queryExpl);
            ComplexExplanation minExpl = new ComplexExplanation(true, Math.min(funcExpl.getValue(), maxBoost), "Math.min of");
            minExpl.addDetail(funcExpl);
            minExpl.addDetail(new Explanation(maxBoost, "maxBoost"));
            res.addDetail(minExpl);
            res.addDetail(new Explanation(queryBoost, "queryBoost"));
            return res;
        }
    },
    REPLACE {
        @Override
        public float combine(double queryBoost, double queryScore, double funcScore, double maxBoost) {
            return toFloat(queryBoost * Math.min(funcScore, maxBoost));
        }

        @Override
        public String getName() {
            return "replace";
        }

        @Override
        public ComplexExplanation explain(float queryBoost, Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            float score = queryBoost * Math.min(funcExpl.getValue(), maxBoost);
            ComplexExplanation res = new ComplexExplanation(true, score, "function score, product of:");
            ComplexExplanation minExpl = new ComplexExplanation(true, Math.min(funcExpl.getValue(), maxBoost), "Math.min of");
            minExpl.addDetail(funcExpl);
            minExpl.addDetail(new Explanation(maxBoost, "maxBoost"));
            res.addDetail(minExpl);
            res.addDetail(new Explanation(queryBoost, "queryBoost"));
            return res;
        }

    },
    SUM {
        @Override
        public float combine(double queryBoost, double queryScore, double funcScore, double maxBoost) {
            return toFloat(queryBoost * (queryScore + Math.min(funcScore, maxBoost)));
        }

        @Override
        public String getName() {
            return "sum";
        }

        @Override
        public ComplexExplanation explain(float queryBoost, Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            float score = queryBoost * (Math.min(funcExpl.getValue(), maxBoost) + queryExpl.getValue());
            ComplexExplanation res = new ComplexExplanation(true, score, "function score, product of:");
            ComplexExplanation minExpl = new ComplexExplanation(true, Math.min(funcExpl.getValue(), maxBoost), "Math.min of");
            minExpl.addDetail(funcExpl);
            minExpl.addDetail(new Explanation(maxBoost, "maxBoost"));
            ComplexExplanation sumExpl = new ComplexExplanation(true, Math.min(funcExpl.getValue(), maxBoost) + queryExpl.getValue(),
                    "sum of");
            sumExpl.addDetail(queryExpl);
            sumExpl.addDetail(minExpl);
            res.addDetail(sumExpl);
            res.addDetail(new Explanation(queryBoost, "queryBoost"));
            return res;
        }

    },
    AVG {
        @Override
        public float combine(double queryBoost, double queryScore, double funcScore, double maxBoost) {
            return toFloat((queryBoost * (Math.min(funcScore, maxBoost) + queryScore) / 2.0));
        }

        @Override
        public String getName() {
            return "avg";
        }

        @Override
        public ComplexExplanation explain(float queryBoost, Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            float score = toFloat(queryBoost * (queryExpl.getValue() + Math.min(funcExpl.getValue(), maxBoost)) / 2.0);
            ComplexExplanation res = new ComplexExplanation(true, score, "function score, product of:");
            ComplexExplanation minExpl = new ComplexExplanation(true, Math.min(funcExpl.getValue(), maxBoost), "Math.min of");
            minExpl.addDetail(funcExpl);
            minExpl.addDetail(new Explanation(maxBoost, "maxBoost"));
            ComplexExplanation avgExpl = new ComplexExplanation(true,
                    toFloat((Math.min(funcExpl.getValue(), maxBoost) + queryExpl.getValue()) / 2.0), "avg of");
            avgExpl.addDetail(queryExpl);
            avgExpl.addDetail(minExpl);
            res.addDetail(avgExpl);
            res.addDetail(new Explanation(queryBoost, "queryBoost"));
            return res;
        }

    },
    MIN {
        @Override
        public float combine(double queryBoost, double queryScore, double funcScore, double maxBoost) {
            return toFloat(queryBoost * Math.min(queryScore, Math.min(funcScore, maxBoost)));
        }

        @Override
        public String getName() {
            return "min";
        }

        @Override
        public ComplexExplanation explain(float queryBoost, Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            float score = toFloat(queryBoost * Math.min(queryExpl.getValue(), Math.min(funcExpl.getValue(), maxBoost)));
            ComplexExplanation res = new ComplexExplanation(true, score, "function score, product of:");
            ComplexExplanation innerMinExpl = new ComplexExplanation(true, Math.min(funcExpl.getValue(), maxBoost), "Math.min of");
            innerMinExpl.addDetail(funcExpl);
            innerMinExpl.addDetail(new Explanation(maxBoost, "maxBoost"));
            ComplexExplanation outerMinExpl = new ComplexExplanation(true, Math.min(Math.min(funcExpl.getValue(), maxBoost),
                    queryExpl.getValue()), "min of");
            outerMinExpl.addDetail(queryExpl);
            outerMinExpl.addDetail(innerMinExpl);
            res.addDetail(outerMinExpl);
            res.addDetail(new Explanation(queryBoost, "queryBoost"));
            return res;
        }

    },
    MAX {
        @Override
        public float combine(double queryBoost, double queryScore, double funcScore, double maxBoost) {
            return toFloat(queryBoost * (Math.max(queryScore, Math.min(funcScore, maxBoost))));
        }

        @Override
        public String getName() {
            return "max";
        }

        @Override
        public ComplexExplanation explain(float queryBoost, Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            float score = toFloat(queryBoost * Math.max(queryExpl.getValue(), Math.min(funcExpl.getValue(), maxBoost)));
            ComplexExplanation res = new ComplexExplanation(true, score, "function score, product of:");
            ComplexExplanation innerMinExpl = new ComplexExplanation(true, Math.min(funcExpl.getValue(), maxBoost), "Math.min of");
            innerMinExpl.addDetail(funcExpl);
            innerMinExpl.addDetail(new Explanation(maxBoost, "maxBoost"));
            ComplexExplanation outerMaxExpl = new ComplexExplanation(true, Math.max(Math.min(funcExpl.getValue(), maxBoost),
                    queryExpl.getValue()), "max of");
            outerMaxExpl.addDetail(queryExpl);
            outerMaxExpl.addDetail(innerMinExpl);
            res.addDetail(outerMaxExpl);
            res.addDetail(new Explanation(queryBoost, "queryBoost"));
            return res;
        }

    };

    public abstract float combine(double queryBoost, double queryScore, double funcScore, double maxBoost);

    public abstract String getName();

    public static float toFloat(double input) {
        assert deviation(input) <= 0.001 : "input " + input + " out of float scope for function score deviation: " + deviation(input);
        return (float) input;
    }

    private static double deviation(double input) { // only with assert!
        float floatVersion = (float) input;
        return Double.compare(floatVersion, input) == 0 || input == 0.0d ? 0 : 1.d - (floatVersion) / input;
    }

    public abstract ComplexExplanation explain(float queryBoost, Explanation queryExpl, Explanation funcExpl, float maxBoost);
}
