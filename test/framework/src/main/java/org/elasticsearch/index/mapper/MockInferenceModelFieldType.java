/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Map;

public class MockInferenceModelFieldType extends SimpleMappedFieldType implements InferenceModelFieldType {
    private static final String TYPE_NAME = "mock_inference_model_field_type";

    private final String modelId;

    public MockInferenceModelFieldType(String name, String modelId) {
        super(name, false, false, false, TextSearchInfo.NONE, Map.of());
        this.modelId = modelId;
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        throw new IllegalArgumentException("termQuery not implemented");
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        return SourceValueFetcher.toString(name(), context, format);
    }

    @Override
    public String getInferenceId() {
        return modelId;
    }
}
