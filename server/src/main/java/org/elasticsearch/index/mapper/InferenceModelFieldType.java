/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

/**
 * Field type that uses an inference model.
 */
public interface InferenceModelFieldType {
    /**
     * Retrieve inference model used by the field type.
     *
     * @return model id used by the field type
     */
    String getInferenceModel();
}
