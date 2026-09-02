/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.index.query.SearchExecutionContextHelper.SHARD_SEARCH_STATS;
import static org.hamcrest.Matchers.equalTo;

public class ExpressionQueryListCircuitBreakerTests extends ESTestCase {

    private static TestThreadPool threadPool;

    @BeforeClass
    public static void init() {
        threadPool = new TestThreadPool("ExpressionQueryListCircuitBreakerTests");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        terminate(threadPool);
        threadPool = null;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    public void testCBReleasedAfterEachGetQueryCall() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        // Empty index – we only need a valid IndexSearcher for the context.
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()))) {
            // intentionally empty
        }

        var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);

        try (
            DirectoryReader reader = DirectoryReader.open(dir);
            var clusterService = ClusterServiceUtils.createClusterService(
                threadPool,
                new ClusterSettings(Settings.EMPTY, registeredSettings)
            )
        ) {

            IndexSearcher searcher = new IndexSearcher(reader);
            IndexVersion indexVersion = IndexVersion.current();
            Settings indexSettingsSettings = indexSettings(indexVersion, 1, 1).build();
            IndexSettings indexSettings = new IndexSettings(
                IndexMetadata.builder("test").settings(indexSettingsSettings).build(),
                Settings.EMPTY
            );
            KeywordFieldMapper fieldMapper = new KeywordFieldMapper.Builder("field", indexSettings).build(
                MapperBuilderContext.root(false, false)
            );
            MappingLookup mappingLookup = MappingLookup.fromMappers(
                Mapping.EMPTY,
                List.of(fieldMapper),
                Collections.emptyList(),
                IndexMode.STANDARD
            );

            SearchExecutionContext baseCtx = new SearchExecutionContext(
                0,
                0,
                indexSettings,
                null,
                null,
                null,
                mappingLookup,
                null,
                null,
                parserConfig(),
                writableRegistry(),
                null,
                searcher,
                System::currentTimeMillis,
                null,
                null,
                () -> true,
                null,
                Collections.emptyMap(),
                null,
                MapperMetrics.NOOP,
                SHARD_SEARCH_STATS
            );

            CircuitBreaker cb = newLimitedBreaker(ByteSizeValue.ofMb(100));
            SearchExecutionContext ctx = new SearchExecutionContext(baseCtx, cb);
            WildcardQueryBuilder pushedQuery = new WildcardQueryBuilder("field", "*test*pattern*");
            ExpressionQueryList queryList = ExpressionQueryList.fieldBasedJoin(
                Collections.emptyList(),
                ctx,
                null,
                pushedQuery,
                clusterService,
                AliasFilter.EMPTY
            );

            assertEquals("CB must be zero before any getQuery() call", 0L, cb.getUsed());

            int positions = 10;
            for (int i = 0; i < positions; i++) {
                long cbBefore = cb.getUsed();

                queryList.getQuery(i, null, ctx);

                assertThat(
                    "CB tracked bytes must be zero after getQuery() (position " + i + ")",
                    ctx.getQueryConstructionMemoryUsed(),
                    equalTo(0L)
                );
                assertThat("Raw CB usage must return to baseline after getQuery() (position " + i + ")", cb.getUsed(), equalTo(cbBefore));
            }
            assertThat("No CB bytes should remain after all positions", cb.getUsed(), equalTo(0L));
        }
    }
}
