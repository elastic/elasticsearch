/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.blockloader;

import org.elasticsearch.index.mapper.MappedFieldType;

/**
 * {@link org.elasticsearch.xpack.esql.core.expression.function.Function}s that can be implemented as part of value loading implement this
 * interface to provide the {@link MappedFieldType.BlockLoaderFunction} that will be used to load and transform the value of the field.
 * @param <T> configuration type for the BlockLoaderFunction
 */
public interface BlockLoaderFunctionProvider<T> {

    /**
     * Returns the function that will be used to load the value of the field and transform it
     */
    MappedFieldType.BlockLoaderFunction<T> getBlockLoaderFunction();
}
