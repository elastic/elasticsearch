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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum CombineFunction implements Writeable {
    MULTIPLY {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return (float) (queryScore * Math.min(funcScore, maxBoost));
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
            return (float) (Math.min(funcScore, maxBoost));
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
            return (float) (queryScore + Math.min(funcScore, maxBoost));
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
            return (float) ((Math.min(funcScore, maxBoost) + queryScore) / 2.0);
        }

        @Override
        public Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost) {
            Explanation minExpl = Explanation.match(Math.min(funcExpl.getValue(), maxBoost), "min of:",
                    funcExpl, Explanation.match(maxBoost, "maxBoost"));
            return Explanation.match(
                    (float) ((Math.min(funcExpl.getValue(), maxBoost) + queryExpl.getValue()) / 2.0), "avg of",
                    queryExpl, minExpl);
        }

    },
    MIN {
        @Override
        public float combine(double queryScore, double funcScore, double maxBoost) {
            return (float) (Math.min(queryScore, Math.min(funcScore, maxBoost)));
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
            return (float) (Math.max(queryScore, Math.min(funcScore, maxBoost)));
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

    public abstract Explanation explain(Explanation queryExpl, Explanation funcExpl, float maxBoost);

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static CombineFunction readFromStream(StreamInput in) throws IOException {
        return in.readEnum(CombineFunction.class);
    }

    public static CombineFunction fromString(String combineFunction) {
        return valueOf(combineFunction.toUpperCase(Locale.ROOT));
    }
}
