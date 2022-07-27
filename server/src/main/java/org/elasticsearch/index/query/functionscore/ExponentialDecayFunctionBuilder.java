/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class ExponentialDecayFunctionBuilder extends DecayFunctionBuilder<ExponentialDecayFunctionBuilder> {
    public static final String NAME = "exp";
    public static final ScoreFunctionParser<ExponentialDecayFunctionBuilder> PARSER = new DecayFunctionParser<>(
        ExponentialDecayFunctionBuilder::new
    );
    public static final DecayFunction EXP_DECAY_FUNCTION = new ExponentialDecayScoreFunction();

    public ExponentialDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset) {
        super(fieldName, origin, scale, offset);
    }

    public ExponentialDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, double decay) {
        super(fieldName, origin, scale, offset, decay);
    }

    ExponentialDecayFunctionBuilder(String fieldName, BytesReference functionBytes) {
        super(fieldName, functionBytes);
    }

    /**
     * Read from a stream.
     */
    public ExponentialDecayFunctionBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getName() {
        return NAME;
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
            return Explanation.match((float) evaluate(value, scale), "exp(- " + valueExpl + " * " + -1 * scale + ")");
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

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }
}
