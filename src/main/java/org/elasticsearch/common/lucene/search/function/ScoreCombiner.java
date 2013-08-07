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

public final class ScoreCombiner {

    public static enum CombineFunction {
        MULT {
            @Override
            public float combine(double queryBoost, double queryScore, double funcScore, double maxBoost) {
                return toFloat(queryBoost * queryScore * Math.min(funcScore, maxBoost));
            }

            @Override
            public String toString() {
                return "mult";
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
        PLAIN {
            @Override
            public float combine(double queryBoost, double queryScore, double funcScore, double maxBoost) {
                return toFloat(queryBoost * Math.min(funcScore, maxBoost));
            }

            @Override
            public String toString() {
                return "plain";
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

        };

        public abstract float combine(double queryBoost, double queryScore, double funcScore, double maxBoost);

        public static float toFloat(double input) {
            assert deviation(input) <= 0.001 : "input " + input + " out of float scope for function score deviation: " + deviation(input);
            return (float) input;
        }
        
        private static double deviation(double input) { // only with assert!
            float floatVersion = (float)input;
            return Double.compare(floatVersion, input) == 0 || input == 0.0d ? 0 : 1.d-(floatVersion) / input;
        }

        public abstract ComplexExplanation explain(float queryBoost, Explanation queryExpl, Explanation funcExpl, float maxBoost);
    }
}
