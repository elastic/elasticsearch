/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractors;

import java.util.ArrayList;
import java.util.List;

public final class SqlHitExtractors {

    private SqlHitExtractors() {}

    /**
     * All of the named writeables needed to deserialize the instances of
     * {@linkplain HitExtractor}.
     */
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(HitExtractors.getNamedWriteables());
        entries.add(new Entry(HitExtractor.class, FieldHitExtractor.NAME, FieldHitExtractor::new));
        entries.add(new Entry(HitExtractor.class, ScoreExtractor.NAME, in -> ScoreExtractor.INSTANCE));
        return entries;
    }
}
