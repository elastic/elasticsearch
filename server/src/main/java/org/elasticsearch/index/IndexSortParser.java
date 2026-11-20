/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.search.SortField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Parses the {@link IndexSortConfig#INDEX_SORT_SETTING} expression, which uses the ES|QL {@code SORT} syntax, and resolves its
 * keys that are computed by an expression. The ES|QL plugin provides it, and node construction loads it as a service provider.
 */
public interface IndexSortParser {

    /**
     * Parses the expression into its sort entries, checking computed keys without the mapping.
     *
     * @throws IllegalArgumentException if the expression is invalid
     */
    List<IndexSortConfig.SortEntry> parse(String expression);

    /**
     * Builds the Lucene sort field of a computed key, which computes the key of each document when Lucene sorts a segment at
     * flush and merge time.
     *
     * @throws IllegalArgumentException if the key is invalid for the mapping
     */
    SortField resolve(
        String computedKey,
        boolean reverse,
        boolean nullsFirst,
        IndexSettings indexSettings,
        Function<String, MappedFieldType> fieldTypeLookup
    );

    /** The parser of this node, or {@code null} without the ES|QL plugin. */
    @Nullable
    static IndexSortParser get() {
        return Holder.PARSER.get();
    }

    /**
     * Sets the parser of this node. Nodes that share a JVM, as in tests, set a parser of the same class.
     */
    static void init(IndexSortParser parser) {
        if (Holder.PARSER.compareAndSet(null, parser) == false && Holder.PARSER.get().getClass() != parser.getClass()) {
            throw new IllegalStateException("index sort parser is already set to [" + Holder.PARSER.get() + "]");
        }
    }

    final class Holder {
        private static final AtomicReference<IndexSortParser> PARSER = new AtomicReference<>();

        private Holder() {}
    }
}
