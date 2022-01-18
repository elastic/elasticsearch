/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

public class PutTrainedModelAliasRequest implements Validatable {

    public static final String REASSIGN = "reassign";

    private final String modelAlias;
    private final String modelId;
    private final Boolean reassign;

    public PutTrainedModelAliasRequest(String modelAlias, String modelId, Boolean reassign) {
        this.modelAlias = Objects.requireNonNull(modelAlias);
        this.modelId = Objects.requireNonNull(modelId);
        this.reassign = reassign;
    }

    public String getModelAlias() {
        return modelAlias;
    }

    public String getModelId() {
        return modelId;
    }

    public Boolean getReassign() {
        return reassign;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutTrainedModelAliasRequest request = (PutTrainedModelAliasRequest) o;
        return Objects.equals(modelAlias, request.modelAlias)
            && Objects.equals(modelId, request.modelId)
            && Objects.equals(reassign, request.reassign);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelAlias, modelId, reassign);
    }

}
