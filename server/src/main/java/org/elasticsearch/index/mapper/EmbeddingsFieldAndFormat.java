/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.search.fetch.subphase.FieldAndFormat;

/**
 * Describes how to fetch embeddings for a field. {@link #useDocValues()} selects Lucene doc values
 * ({@code docvalue_fields}) versus the {@code fields} API (typically {@code _source}).
 */
public record EmbeddingsFieldAndFormat(FieldAndFormat fieldAndFormat, boolean useDocValues) {

    public static EmbeddingsFieldAndFormat fields(String field, String format) {
        return new EmbeddingsFieldAndFormat(new FieldAndFormat(field, format), false);
    }

    public static EmbeddingsFieldAndFormat docValues(String field, String format) {
        return new EmbeddingsFieldAndFormat(new FieldAndFormat(field, format), true);
    }
}
