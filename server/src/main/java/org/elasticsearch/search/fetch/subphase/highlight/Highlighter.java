/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

/**
 * Highlights a search result.
 */
public interface Highlighter {
    boolean canHighlight(MappedFieldType fieldType);

    /**
     * Whether this highlighter can highlight the given field without {@code _source} (sourcing its content from doc values or
     * stored fields instead). When {@code true} for a non-stored field, {@link HighlightPhase} does not request {@code _source},
     * avoiding the cost of rebuilding it in synthetic-source and columnar indices. Defaults to {@code false}.
     */
    default boolean canHighlightWithoutSource(MappedFieldType fieldType, SearchExecutionContext context) {
        return false;
    }

    HighlightField highlight(FieldHighlightContext fieldContext) throws IOException;
}
