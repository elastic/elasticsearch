/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.search.Explanation;

public enum CombineFunction {
    MULT {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return toFloat(queryScore * Math.min(funcScore, maxBoost));
        }

        @Override
        public String getName() {
            return "multiply";
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation boostExpl = Explanation.match(maxBoost, "maxBoost");
            Explanation minExpl = Explanation.match(
                    Math.min(funcExpl.getValue(), maxBoost),
                    "min of:",
                    funcExpl, boostExpl);
            return Explanation.match(queryExpl.getValue() * minExpl.getValue(),
                    "function score, product of:", queryExpl, minExpl);
        }
    },
    REPLACE {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return toFloat(Math.min(funcScore, maxBoost));
        }

        @Override
        public String getName() {
            return "replace";
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation boostExpl = Explanation.match(maxBoost, "maxBoost");
            return Explanation.match(
                    Math.min(funcExpl.getValue(), maxBoost),
                    "min of:",
                    funcExpl, boostExpl);
        }

    },
    SUM {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return toFloat(queryScore + Math.min(funcScore, maxBoost));
        }

        @Override
        public String getName() {
            return "sum";
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation minExpl = Explanation.match(Math.min(funcExpl.getValue(), maxBoost), "min of:",
                    funcExpl, Explanation.match(maxBoost, "maxBoost"));
            return Explanation.match(Math.min(funcExpl.getValue(), maxBoost) + queryExpl.getValue(), "sum of",
                    queryExpl, minExpl);
        }

    },
    AVG {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return toFloat((Math.min(funcScore, maxBoost) + queryScore) / 2.0);
        }

        @Override
        public String getName() {
            return "avg";
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation minExpl = Explanation.match(Math.min(funcExpl.getValue(), maxBoost), "min of:",
                    funcExpl, Explanation.match(maxBoost, "maxBoost"));
            return Explanation.match(
                    toFloat((Math.min(funcExpl.getValue(), maxBoost) + queryExpl.getValue()) / 2.0), "avg of",
                    queryExpl, minExpl);
        }

    },
    MIN {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return toFloat(Math.min(queryScore, Math.min(funcScore, maxBoost)));
        }

        @Override
        public String getName() {
            return "min";
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation innerMinExpl = Explanation.match(
                    Math.min(funcExpl.getValue(), maxBoost), "min of:",
                    funcExpl, Explanation.match(maxBoost, "maxBoost"));
            return Explanation.match(
                    Math.min(Math.min(funcExpl.getValue(), maxBoost), queryExpl.getValue()), "min of",
                    queryExpl, innerMinExpl);
        }

    },
    MAX {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return toFloat(Math.max(queryScore, Math.min(funcScore, maxBoost)));
        }

        @Override
        public String getName() {
            return "max";
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation innerMinExpl = Explanation.match(
                    Math.min(funcExpl.getValue(), maxBoost), "min of:",
                    funcExpl, Explanation.match(maxBoost, "maxBoost"));
            return Explanation.match(
                    Math.max(Math.min(funcExpl.getValue(), maxBoost), queryExpl.getValue()), "max of:",
                    queryExpl, innerMinExpl);
        }

    };

    public abstract float combine(double queryScore, double funcScore, double maxBoost);

    public abstract String getName();

    public static float toFloat(double input) {
        assert deviation(input) <= 0.001 : "input " + input + " out of float scope for function score deviation: " + deviation(input);
        return (float) input;
    }

    private static double deviation(double input) { // only with assert!
        float floatVersion = (float) input;
        return Double.compare(floatVersion, input) == 0 || input == 0.0d ? 0 : 1.d - (floatVersion) / input;
    }

    public abstract Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost);
}
