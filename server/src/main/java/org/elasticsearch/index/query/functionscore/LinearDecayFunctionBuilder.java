/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class LinearDecayFunctionBuilder extends DecayFunctionBuilder<LinearDecayFunctionBuilder> {
    public static final String NAME = "linear";
    public static final ScoreFunctionParser<LinearDecayFunctionBuilder> PARSER = new DecayFunctionParser<>(LinearDecayFunctionBuilder::new);
    public static final DecayFunction LINEAR_DECAY_FUNCTION = new LinearDecayScoreFunction();

    public LinearDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset) {
        super(fieldName, origin, scale, offset);
    }

    public LinearDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, double decay) {
        super(fieldName, origin, scale, offset, decay);
    }

    LinearDecayFunctionBuilder(String fieldName, BytesReference functionBytes) {
        super(fieldName, functionBytes);
    }

    /**
     * Read from a stream.
     */
    public LinearDecayFunctionBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public DecayFunction getDecayFunction() {
        return LINEAR_DECAY_FUNCTION;
    }

    private static final class LinearDecayScoreFunction implements DecayFunction {

        @Override
        public double evaluate(double value, double scale) {
            return Math.max(0.0, (scale - value) / scale);
        }

        @Override
        public Explanation explainFunction(String valueExpl, double value, double scale) {
            return Explanation.match(
                    (float) evaluate(value, scale),
                    "max(0.0, ((" + scale + " - " + valueExpl + ")/" + scale + ")");
        }

        @Override
        public double processScale(double scale, double decay) {
            return scale / (1.0 - decay);
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
