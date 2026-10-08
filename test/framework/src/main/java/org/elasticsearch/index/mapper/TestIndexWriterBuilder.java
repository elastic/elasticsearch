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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.ElasticsearchCodec;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.fielddata.FieldDataContext;

import java.io.IOException;
import java.util.Objects;

/**
 * Builds a {@link RandomIndexWriter} with the codec and index sort the engine would use for a mapping, with the parent field set
 * whenever it sorts so nested documents stay together. The analyzer defaults to {@link StandardAnalyzer}. Merges only combine
 * adjacent segments, so documents keep their insertion order, which tests rely on when they address documents by id. The
 * configuration is not randomized and never exposed, so a test cannot write doc values in a format, or documents in an order, its
 * mapping would not produce. Any other choice must be explicit: {@link #overrideAnalyzer}, {@link #overrideIndexSort} or
 * {@link #disableMerges}.
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
            productionCodec(mapperService),
            mapperService.getIndexSettings().getIndexVersionCreated(),
            productionIndexSort(mapperService)
        );
    }

    /**
     * The codec the engine resolves for {@code mapperService}: {@code index.codec}, or the index mode's default codec.
     */
    static Codec productionCodec(MapperService mapperService) {
        return new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null).codec(
            EngineConfig.INDEX_CODEC_SETTING.get(mapperService.getIndexSettings().getSettings())
        );
    }

    static DocValuesFormat docValuesFormatForField(Codec codec, String field) {
        if (codec instanceof ElasticsearchCodec elasticsearchCodec) {
            return elasticsearchCodec.getDocValuesFormatForField(field);
        }
        if (codec.docValuesFormat() instanceof PerFieldDocValuesFormat perField) {
            return perField.getDocValuesFormatForField(field);
        }
        throw new AssertionError("codec [" + codec.getName() + "] does not pick doc values formats per field");
    }

    /**
     * Whether a writer built for {@code mapperService} accepts document blocks. Lucene rejects them in a sorted index without a parent
     * field, which the engine only sets from {@link IndexVersions#INDEX_SORTING_ON_NESTED}. With the parent field, a block stays
     * contiguous and in order however the index sorts.
     */
    static boolean acceptsDocumentBlocks(MapperService mapperService) {
        return mapperService.getIndexSettings().getIndexSortConfig().hasIndexSort() == false
            || setsParentField(mapperService.getIndexSettings().getIndexVersionCreated());
    }

    private static boolean setsParentField(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(IndexVersions.INDEX_SORTING_ON_NESTED);
    }

    static Sort productionIndexSort(MapperService mapperService) {
        return new IndexSortConfig(mapperService.getIndexSettings()).buildIndexSort(
            mapperService::fieldType,
            (ft, s) -> ft.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "")).build(null, null)
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
            if (setsParentField(indexVersion)) {
                config.setParentField(Engine.ROOT_DOC_FIELD_NAME);
            }
        }
        config.setMergePolicy(mergesDisabled ? NoMergePolicy.INSTANCE : new LogDocMergePolicy());
        return new RandomIndexWriter(LuceneTestCase.random(), directory, config);
    }
}
