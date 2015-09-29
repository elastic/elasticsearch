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

package org.elasticsearch.index.query.functionscore.exp;


import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.functionscore.DecayFunction;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;

public class ExponentialDecayFunctionBuilder extends DecayFunctionBuilder<ExponentialDecayFunctionBuilder> {

    private static final DecayFunction EXP_DECAY_FUNCTION = new ExponentialDecayScoreFunction();

    public ExponentialDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset) {
        super(fieldName, origin, scale, offset);
    }

    public ExponentialDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, double decay) {
        super(fieldName, origin, scale, offset, decay);
    }

    private ExponentialDecayFunctionBuilder(String fieldName, BytesReference functionBytes) {
        super(fieldName, functionBytes);
    }

    @Override
    protected ExponentialDecayFunctionBuilder createFunctionBuilder(String fieldName, BytesReference functionBytes) {
        return new ExponentialDecayFunctionBuilder(fieldName, functionBytes);
    }

    @Override
    public String getName() {
        return ExponentialDecayFunctionParser.NAMES[0];
    }

    @Override
    public DecayFunction getDecayFunction() {
        return EXP_DECAY_FUNCTION;
    }

    private static final class ExponentialDecayScoreFunction implements DecayFunction {

        @Override
        public double evaluate(double value, double scale) {
            return Math.exp(scale * value);
        }

        @Override
        public Explanation explainFunction(String valueExpl, double value, double scale) {
            return Explanation.match(
                    (float) evaluate(value, scale),
                    "exp(- " + valueExpl + " * " + -1 * scale + ")");
        }

        @Override
        public double processScale(double scale, double decay) {
            return Math.log(decay) / scale;
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
