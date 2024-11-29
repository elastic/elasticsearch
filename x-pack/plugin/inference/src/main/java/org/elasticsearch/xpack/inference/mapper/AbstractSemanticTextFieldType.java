/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.InferenceResults;

import java.util.Map;

public abstract class AbstractSemanticTextFieldType extends SimpleMappedFieldType {
    protected AbstractSemanticTextFieldType(
        String name,
        boolean isIndexed,
        boolean isStored,
        boolean hasDocValues,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta
    ) {
        super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
    }

    public abstract QueryBuilder semanticQuery(InferenceResults inferenceResults, Integer requestSize, float boost, String queryName);
}
