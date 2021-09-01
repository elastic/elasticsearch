/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class EstimateModelMemoryResponse {

    public static final ParseField MODEL_MEMORY_ESTIMATE = new ParseField("model_memory_estimate");

    static final ConstructingObjectParser<EstimateModelMemoryResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "estimate_model_memory",
            true,
            args -> new EstimateModelMemoryResponse((String) args[0]));

    static {
        PARSER.declareString(constructorArg(), MODEL_MEMORY_ESTIMATE);
    }

    public static EstimateModelMemoryResponse fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final ByteSizeValue modelMemoryEstimate;

    public EstimateModelMemoryResponse(String modelMemoryEstimate) {
        this.modelMemoryEstimate = ByteSizeValue.parseBytesSizeValue(modelMemoryEstimate, MODEL_MEMORY_ESTIMATE.getPreferredName());
    }

    /**
     * @return An estimate of the model memory the supplied analysis config is likely to need given the supplied field cardinalities.
     */
    public ByteSizeValue getModelMemoryEstimate() {
        return modelMemoryEstimate;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EstimateModelMemoryResponse other = (EstimateModelMemoryResponse) o;
        return Objects.equals(this.modelMemoryEstimate, other.modelMemoryEstimate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelMemoryEstimate);
    }
}
