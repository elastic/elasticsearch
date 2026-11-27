/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * The time column of an ES|QL datafeed as {@code DESCRIBE} reports it, once {@link EsqlDatafeedQueryValidator#checkDescription} has
 * accepted it. The column carries the job {@code time_field} as its name and is not a renamed field, so the datafeed filters the
 * source on that same name: the column is the mapped field itself, or a value computed from the source field of that name, such as
 * {@code BY @timestamp = BUCKET(@timestamp, 1 hour)}.
 *
 * @param name         the column name, which is the job {@code time_field} and the field the source range filter targets
 * @param origin       the {@code DESCRIBE} origin, for example {@code mapped} or {@code computed}, or {@code null} when not reported
 * @param bucketMillis the length of the interval the query floors the column to, or 0 when the query emits source timestamps
 */
record TimeColumn(String name, @Nullable String origin, long bucketMillis) {

    /** The {@code DESCRIBE} origin of a field under another name. The datafeed cannot filter the source on such a column. */
    static final String RENAMED_ORIGIN = "renamed";

    TimeColumn {
        Objects.requireNonNull(name);
        assert RENAMED_ORIGIN.equals(origin) == false : "a renamed time column cannot filter the source";
        assert bucketMillis >= 0 : "negative bucket length " + bucketMillis;
    }
}
