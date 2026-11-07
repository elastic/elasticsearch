/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import static org.elasticsearch.xpack.querysampling.storage.IndexDefinitions.field;

/**
 * Durable home of the sampled queries, one document per query. A document belongs to one sampler run because its
 * weights only make sense against that sampler's counts; documents of different samplers are combined on read.
 */
public final class QuerySamplingIndex {

    public static final String NAME = ".query_sampling";

    static final int MAPPINGS_VERSION = 1;

    private QuerySamplingIndex() {}

    public static SystemIndexDescriptor descriptor() {
        return IndexDefinitions.descriptor(NAME, "Contains the sampled kNN queries used to estimate the quality of search", mappings());
    }

    static XContentBuilder mappings() {
        return IndexDefinitions.mappings(NAME, MAPPINGS_VERSION, builder -> {
            IndexDefinitions.identityProperties(builder, true);
            field(builder, "picked_at", "date");
            field(builder, "updated_at", "date");
            field(builder, "has_ground_truth", "boolean");
            IndexDefinitions.payloadProperties(builder);
        });
    }
}
