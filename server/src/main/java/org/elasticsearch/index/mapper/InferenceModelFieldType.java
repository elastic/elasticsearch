/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Map;

/**
 * Field type that uses an inference model.
 */
// TODO: Are there any scenarios where extending SimpleMappedFieldType becomes an issue?
public abstract class InferenceModelFieldType extends SimpleMappedFieldType {
    public InferenceModelFieldType(
        String name,
        boolean isIndexed,
        boolean isStored,
        boolean hasDocValues,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta
    ) {
        super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
    }

    /**
     * Retrieve inference model used by the field type.
     *
     * @return model id used by the field type
     */
    public abstract String getInferenceModel();
}
