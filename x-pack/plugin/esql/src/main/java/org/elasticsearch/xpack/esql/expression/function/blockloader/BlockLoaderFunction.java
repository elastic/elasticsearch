/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.blockloader;

import org.elasticsearch.index.mapper.MappedFieldType;


public interface BlockLoaderFunction {

    /**
     * Returns the function that will be used to load the value of the field and transform it
     */
    MappedFieldType.BlockLoaderValueFunction<?, ?> getBlockLoaderValueFunction();
}
