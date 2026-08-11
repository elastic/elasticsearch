/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * The approximate size class of a catalog {@link SourceVariant}'s resource, used only for reporting and
 * for choosing which variants a quick manual run should target; it has no effect on how the runner reads
 * the resource.
 */
public enum DataScale {
    /** Roughly megabytes; suitable for every manual run and any future dedicated-CI smoke shard. */
    SMOKE,
    /** Roughly tens to low hundreds of megabytes. */
    MEDIUM,
    /** Gigabytes or more; the full, unsliced upstream corpus. */
    LARGE;

    public static DataScale parse(String value) {
        return DataScale.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
