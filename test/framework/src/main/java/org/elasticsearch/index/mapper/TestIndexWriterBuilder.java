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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.Engine;

import java.io.IOException;
import java.util.Objects;

/**
 * Builds a {@link RandomIndexWriter} with the codec and index sort the engine would use for a mapping, with the parent field set
 * whenever it sorts so nested documents stay together. The analyzer defaults to {@link StandardAnalyzer}. The configuration is not
 * randomized and never exposed, so a test cannot write doc values in a format, or documents in an order, its mapping would not
 * produce. Any other choice must be explicit: {@link #overrideAnalyzer}, {@link #overrideIndexSort} or {@link #disableMerges}.
 */
public final class TestIndexWriterBuilder {

    private final Codec codec;
    private final IndexVersion indexVersion;
    private Analyzer analyzer = new StandardAnalyzer();
    private Sort indexSort;
    private boolean mergesDisabled;

    private TestIndexWriterBuilder(Codec codec, IndexVersion indexVersion, Sort indexSort) {
        this.codec = codec;
        this.indexVersion = indexVersion;
        this.indexSort = indexSort;
    }

    public static TestIndexWriterBuilder mapped(MapperService mapperService) {
        Objects.requireNonNull(mapperService);
        return new TestIndexWriterBuilder(
            MapperServiceTestCase.productionCodec(mapperService),
            mapperService.getIndexSettings().getIndexVersionCreated(),
            MapperServiceTestCase.productionIndexSort(mapperService)
        );
    }

    public TestIndexWriterBuilder overrideAnalyzer(Analyzer analyzer) {
        this.analyzer = Objects.requireNonNull(analyzer);
        return this;
    }

    public TestIndexWriterBuilder overrideIndexSort(Sort indexSort) {
        this.indexSort = Objects.requireNonNull(indexSort);
        return this;
    }

    public TestIndexWriterBuilder disableMerges() {
        this.mergesDisabled = true;
        return this;
    }

    public RandomIndexWriter build(Directory directory) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setCodec(codec);
        if (indexSort != null) {
            config.setIndexSort(indexSort);
            if (indexVersion.onOrAfter(IndexVersions.INDEX_SORTING_ON_NESTED)) {
                config.setParentField(Engine.ROOT_DOC_FIELD_NAME);
            }
        }
        if (mergesDisabled) {
            config.setMergePolicy(NoMergePolicy.INSTANCE);
        }
        return new RandomIndexWriter(LuceneTestCase.random(), directory, config);
    }
}
