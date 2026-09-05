/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/** Physical object layouts the matrix crosses. */
public enum Layout {
    SINGLE_FILE("single", false),
    UNIFORM_SHARDS("shards", true),
    SKEWED_SHARDS("skewed", true),
    MANY_SMALL("small", true),
    /** A single very wide file whose row groups stress reader memory; still one object. */
    WIDE_SINGLE_ROW_GROUP("widerg", false),
    HIVE_PARTITIONED("hive", true),
    NESTED_HIVE("nested-hive", true);

    private final String labelId;
    private final boolean multiFile;

    Layout(String labelId, boolean multiFile) {
        this.labelId = labelId;
        this.multiFile = multiFile;
    }

    /** Short id used in variant labels (part of the JUnit test name). */
    public String labelId() {
        return labelId;
    }

    /** Whether this layout requires listing/globbing, i.e. is blocked on providers without it. */
    public boolean multiFile() {
        return multiFile;
    }

    public static Layout fromId(String id) {
        return valueOf(id.toUpperCase(Locale.ROOT));
    }
}
