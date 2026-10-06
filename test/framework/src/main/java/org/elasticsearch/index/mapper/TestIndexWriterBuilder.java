/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;
import java.util.Objects;

/**
 * Builds a {@link RandomIndexWriter} that writes with the codec production picks for a mapping. The codec is fixed by
 * {@link #mapped} and the configuration is never exposed, so a test cannot write doc values in a format its mapping would not.
 */
public final class TestIndexWriterBuilder {

    private final Codec codec;
    private Analyzer analyzer;
    private Sort indexSort;
    private MergePolicy mergePolicy;

    private TestIndexWriterBuilder(Codec codec) {
        this.codec = codec;
    }

    public static TestIndexWriterBuilder mapped(MapperService mapperService) {
        return new TestIndexWriterBuilder(MapperServiceTestCase.productionCodec(Objects.requireNonNull(mapperService)));
    }

    public TestIndexWriterBuilder analyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public TestIndexWriterBuilder indexSort(Sort indexSort) {
        this.indexSort = indexSort;
        return this;
    }

    public TestIndexWriterBuilder mergePolicy(MergePolicy mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    public RandomIndexWriter build(Directory directory) throws IOException {
        final IndexWriterConfig config = analyzer == null
            ? LuceneTestCase.newIndexWriterConfig()
            : LuceneTestCase.newIndexWriterConfig(LuceneTestCase.random(), analyzer);
        config.setCodec(codec);
        if (indexSort != null) {
            config.setIndexSort(indexSort);
        }
        if (mergePolicy != null) {
            config.setMergePolicy(mergePolicy);
        }
        return new RandomIndexWriter(LuceneTestCase.random(), directory, config);
    }
}
