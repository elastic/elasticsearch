/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts a columns value from a {@link SearchHit}.
 */
public interface HitExtractor extends NamedWriteable {
    /**
     * All of the named writeables needed to deserialize the instances
     * of {@linkplain HitExtractor}.
     */
    static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new Entry(HitExtractor.class, ConstantExtractor.NAME, ConstantExtractor::new));
        entries.add(new Entry(HitExtractor.class, DocValueExtractor.NAME, DocValueExtractor::new));
        entries.add(new Entry(HitExtractor.class, InnerHitExtractor.NAME, InnerHitExtractor::new));
        entries.add(new Entry(HitExtractor.class, SourceExtractor.NAME, SourceExtractor::new));
        entries.add(new Entry(HitExtractor.class, ProcessingHitExtractor.NAME, ProcessingHitExtractor::new));
        entries.addAll(ColumnProcessor.getNamedWriteables());
        return entries;
    }

    /**
     * Extract the value from a hit.
     */
    Object get(SearchHit hit);

    /**
     * Name of the inner hit needed by this extractor if it needs one, {@code null} otherwise.
     */
    @Nullable
    String innerHitName();
}