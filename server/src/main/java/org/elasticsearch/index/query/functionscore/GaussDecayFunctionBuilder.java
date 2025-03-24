/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class GaussDecayFunctionBuilder extends DecayFunctionBuilder<GaussDecayFunctionBuilder> {
    public static final String NAME = "gauss";
    public static final ScoreFunctionParser<GaussDecayFunctionBuilder> PARSER = new DecayFunctionParser<>(GaussDecayFunctionBuilder::new);
    public static final DecayFunction GAUSS_DECAY_FUNCTION = new GaussScoreFunction();

    public GaussDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset) {
        super(fieldName, origin, scale, offset);
    }

    public GaussDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, double decay) {
        super(fieldName, origin, scale, offset, decay);
    }

    GaussDecayFunctionBuilder(String fieldName, BytesReference functionBytes) {
        super(fieldName, functionBytes);
    }

    /**
     * Read from a stream.
     */
    public GaussDecayFunctionBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public DecayFunction getDecayFunction() {
        return GAUSS_DECAY_FUNCTION;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
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
            return Explanation.match((float) evaluate(value, scale), "exp(-0.5*pow(" + valueExpl + ",2.0)/" + -1 * scale + ")");
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
            return obj == this || (obj != null && obj.getClass() == this.getClass());
        }
    }
}
