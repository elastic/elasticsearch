/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * Corpus-level scale bucket, for filtering and runtime budgeting. Two scales of the same logical
 * data are two corpora with two spec files (all variants of one corpus must return identical
 * answers).
 */
public enum Scale {
    /** Comfortably scanned in full on every leg. */
    SMALL,
    /** Full scans are fine, but keep an eye on the logged timings. */
    MEDIUM,
    /** Full scans only on selected legs; others carry query subsets or filters. */
    LARGE,
    /** Never scanned in full: partial-corpus fractions, query subsets and in-query filters. */
    HUGE;

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static Scale fromId(String id) {
        return valueOf(id.toUpperCase(Locale.ROOT));
    }
}
