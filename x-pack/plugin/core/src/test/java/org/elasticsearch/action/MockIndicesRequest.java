/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.action;

import org.elasticsearch.action.support.IndicesOptions;

public class MockIndicesRequest extends ActionRequest implements IndicesRequest, CompositeIndicesRequest {

    private final String[] indices;
    private final IndicesOptions indicesOptions;

    public MockIndicesRequest(IndicesOptions indicesOptions, String... indices) {
        this.indices = indices;
        this.indicesOptions = indicesOptions;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
