/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;

import java.util.ArrayList;
import java.util.List;

public abstract class HitExtractors {

    /**
     * All of the named writeables needed to deserialize the instances of
     * {@linkplain HitExtractor}.
     */
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new Entry(HitExtractor.class, ConstantExtractor.NAME, ConstantExtractor::new));
        entries.add(new Entry(HitExtractor.class, DocValueExtractor.NAME, DocValueExtractor::new));
        entries.add(new Entry(HitExtractor.class, InnerHitExtractor.NAME, InnerHitExtractor::new));
        entries.add(new Entry(HitExtractor.class, SourceExtractor.NAME, SourceExtractor::new));
        entries.add(new Entry(HitExtractor.class, ComputingHitExtractor.NAME, ComputingHitExtractor::new));
        entries.addAll(Processors.getNamedWriteables());
        return entries;
    }
}
