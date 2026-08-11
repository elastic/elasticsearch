/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * The physical object layout of a catalog {@link SourceVariant}'s resource. Every value here is a
 * layout the upstream publisher already exposes; the catalog never repartitions or rewrites an object
 * to manufacture a layout (plan section 3).
 */
public enum PartitionLayout {
    /** Exactly one object. */
    SINGLE_FILE,
    /** Multiple objects of roughly equal size (e.g. a fixed-count Parquet split). */
    UNIFORM_SHARDS,
    /** Multiple objects whose sizes vary widely (e.g. one file per weather station). */
    SKEWED_SHARDS,
    /** Many (dozens to thousands) small objects, e.g. one per day. */
    MANY_SMALL_FILES,
    /** A single level of {@code key=value} directory partitioning (e.g. {@code year=2022/}). */
    HIVE_PARTITIONED,
    /** Two or more nested levels of {@code key=value} directory partitioning. */
    NESTED_HIVE_PARTITIONED;

    public static PartitionLayout parse(String value) {
        return PartitionLayout.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
