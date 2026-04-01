/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query;

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
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class InnerHitContextBuilderCircuitBreakerTests extends ESTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testCBTrackedDuringInnerHitsAndReleasedAtRequestEnd() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        // Empty index – we just need a valid IndexSearcher for the context.
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()))) {
            // intentionally empty
        }

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);

            IndexVersion indexVersion = IndexVersion.current();
            Settings indexSettingsSettings = indexSettings(indexVersion, 1, 1).build();
            IndexSettings indexSettings = new IndexSettings(
                IndexMetadata.builder("test").settings(indexSettingsSettings).build(),
                Settings.EMPTY
            );
            KeywordFieldMapper fieldMapper = new KeywordFieldMapper.Builder("field", indexSettings.getIndexVersionCreated()).build(
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
                MapperMetrics.NOOP
            );

            CircuitBreaker cb = newLimitedBreaker(ByteSizeValue.ofMb(100));
            SearchExecutionContext ctx = new SearchExecutionContext(baseCtx, cb);
            QueryBuilder innerQuery = new WildcardQueryBuilder("field", "*test*pattern*");
            InnerHitBuilder innerHitBuilder = new InnerHitBuilder("test_inner");
            InnerHitContextBuilder builder = new InnerHitContextBuilder(innerQuery, innerHitBuilder, Collections.emptyMap()) {
                @Override
                protected void doBuild(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) {}
            };

            InnerHitsContext.InnerHitSubContext subContext = org.mockito.Mockito.mock(InnerHitsContext.InnerHitSubContext.class);

            long baselineUsed = cb.getUsed();

            int iterations = 5;
            for (int i = 0; i < iterations; i++) {
                builder.setupInnerHitsContext(ctx, subContext);
                assertThat(
                    "CB bytes must still be tracked (not released early) after iteration " + i,
                    cb.getUsed(),
                    greaterThan(baselineUsed)
                );
            }

            ctx.releaseQueryConstructionMemory();
            assertThat("All CB bytes must be released after the request-end release", cb.getUsed(), equalTo(baselineUsed));
        }
    }
}
