/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;

import java.util.ArrayList;
import java.util.List;

public final class HitExtractors {

    private HitExtractors() {}

    /**
     * All of the named writeables needed to deserialize the instances of
     * {@linkplain HitExtractor}.
     */
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new Entry(HitExtractor.class, ConstantExtractor.NAME, ConstantExtractor::new));
        entries.add(new Entry(HitExtractor.class, ComputingExtractor.NAME, ComputingExtractor::new));
        return entries;
    }
}
