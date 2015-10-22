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

package org.elasticsearch.index.query.functionscore.gauss;


import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.functionscore.DecayFunction;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;

public class GaussDecayFunctionBuilder extends DecayFunctionBuilder<GaussDecayFunctionBuilder> {

    public static final DecayFunction GAUSS_DECAY_FUNCTION = new GaussScoreFunction();

    public GaussDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset) {
        super(fieldName, origin, scale, offset);
    }

    public GaussDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, double decay) {
        super(fieldName, origin, scale, offset, decay);
    }

    private GaussDecayFunctionBuilder(String fieldName, BytesReference functionBytes) {
        super(fieldName, functionBytes);
    }

    @Override
    protected GaussDecayFunctionBuilder createFunctionBuilder(String fieldName, BytesReference functionBytes) {
        return new GaussDecayFunctionBuilder(fieldName, functionBytes);
    }

    @Override
    public String getName() {
        return GaussDecayFunctionParser.NAMES[0];
    }

    @Override
    public DecayFunction getDecayFunction() {
        return GAUSS_DECAY_FUNCTION;
    }

    private static final class GaussScoreFunction implements DecayFunction {

        @Override
        public double evaluate(double value, double scale) {
            // note that we already computed scale^2 in processScale() so we do
            // not need to square it here.
            return Math.exp(0.5 * Math.pow(value, 2.0) / scale);
        }

        @Override
        public Explanation explainFunction(String valueExpl, double value, double scale) {
            return Explanation.match(
                    (float) evaluate(value, scale),
                    "exp(-0.5*pow(" + valueExpl + ",2.0)/" + -1 * scale + ")");
        }

        @Override
        public double processScale(double scale, double decay) {
            return 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (super.equals(obj)) {
                return true;
            }
            return obj != null && getClass() != obj.getClass();
        }
    }
}
