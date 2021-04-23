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

public class DeleteTrainedModelAliasRequest implements Validatable {

    private final String modelAlias;
    private final String modelId;

    public DeleteTrainedModelAliasRequest(String modelAlias, String modelId) {
        this.modelAlias = Objects.requireNonNull(modelAlias);
        this.modelId = Objects.requireNonNull(modelId);
    }

    public String getModelAlias() {
        return modelAlias;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteTrainedModelAliasRequest request = (DeleteTrainedModelAliasRequest) o;
        return Objects.equals(modelAlias, request.modelAlias)
            && Objects.equals(modelId, request.modelId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelAlias, modelId);
    }

}
