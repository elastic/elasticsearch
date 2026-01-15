/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ModelCreator;

/**
 * Base class for creating {@link ElasticsearchInternalModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 * @param <M> the type of {@link ElasticsearchInternalModel} created by this creator
 */
public abstract class ElasticsearchInternalModelCreator<M extends ElasticsearchInternalModel> implements ModelCreator<M> {

    public abstract boolean matches(TaskType taskType, String modelId);
}
