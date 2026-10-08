/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;

public class SemanticIndexOptionsIT extends SemanticFieldIndexOptionsTestCase {

    @Override
    protected String fieldType() {
        return SemanticFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected TaskType taskType() {
        return TaskType.EMBEDDING;
    }
}
