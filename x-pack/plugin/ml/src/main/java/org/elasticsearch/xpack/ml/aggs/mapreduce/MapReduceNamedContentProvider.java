/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.EclatMapReducer;

import java.util.List;

/**
 * "Registry" for all map reducers
 */
public final class MapReduceNamedContentProvider {

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(AbstractMapReducer.class, EclatMapReducer.NAME, EclatMapReducer::new));
    }
}
