/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractors;

import java.util.ArrayList;
import java.util.List;

public class SqlBucketExtractors {
 
    private SqlBucketExtractors() {}

    /**
     * All of the named writeables needed to deserialize the instances of
     * {@linkplain BucketExtractor}s.
     */
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(BucketExtractors.getNamedWriteables());
        entries.add(new Entry(BucketExtractor.class, CompositeKeyExtractor.NAME, CompositeKeyExtractor::new));
        entries.add(new Entry(BucketExtractor.class, MetricAggExtractor.NAME, MetricAggExtractor::new));
        entries.add(new Entry(BucketExtractor.class, TopHitsAggExtractor.NAME, TopHitsAggExtractor::new));
        entries.add(new Entry(BucketExtractor.class, PivotExtractor.NAME, PivotExtractor::new));
        return entries;
    }
}
