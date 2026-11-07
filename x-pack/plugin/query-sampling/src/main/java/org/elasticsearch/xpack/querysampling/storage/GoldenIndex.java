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
 * The golden dataset: promoted queries with their ground truth, kept for good (nothing deletes from this index).
 * Records of a version are never changed, so a benchmarked version can be found again as it was. A manifest document
 * numbers each version and says whether it is complete; records and manifests are told apart by {@code kind}.
 */
public final class GoldenIndex {

    public static final String NAME = ".query_golden";

    static final int MAPPINGS_VERSION = 1;

    static final String RECORD = "record";
    static final String VERSION = "version";

    private GoldenIndex() {}

    public static SystemIndexDescriptor descriptor() {
        return IndexDefinitions.descriptor(
            NAME,
            "Contains the golden dataset of kNN queries, with their ground truth, promoted from the sampled queries",
            mappings()
        );
    }

    static XContentBuilder mappings() {
        return IndexDefinitions.mappings(NAME, MAPPINGS_VERSION, builder -> {
            field(builder, "kind", "keyword");
            field(builder, "dataset_version", "long");
            // of a manifest
            field(builder, "created_at", "date");
            field(builder, "records", "long");
            field(builder, "completed", "boolean");
            field(builder, "source_version", "long");
            // of a record
            field(builder, "promoted_at", "date");
            field(builder, "source_picked_at", "date");
            IndexDefinitions.identityProperties(builder, false);
            IndexDefinitions.payloadProperties(builder);
        });
    }
}
