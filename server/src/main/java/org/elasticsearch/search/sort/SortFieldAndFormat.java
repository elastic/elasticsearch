/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.sort;

import org.apache.lucene.search.SortField;
import org.elasticsearch.search.DocValueFormat;

import java.util.Objects;

public record SortFieldAndFormat(SortField field, DocValueFormat format) {

    public SortFieldAndFormat(SortField field, DocValueFormat format) {
        this.field = Objects.requireNonNull(field);
        this.format = Objects.requireNonNull(format);
    }

}
