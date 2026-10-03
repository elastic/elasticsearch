/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SourceFilter;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link FallbackSyntheticSourceBlockLoader}'s row stride reader. When {@code _ignored_source} is stored in binary doc values
 * the reader holds a forward-only doc values iterator, so it must report that it can't be reused for an earlier document, and it has to
 * account for the block buffer of that iterator on the circuit breaker.
 */
public class FallbackSyntheticSourceBlockLoaderTests extends MapperServiceTestCase {

    /**
     * The reader wraps a forward-only {@code _ignored_source} doc values iterator, so {@code canReuse} has to track the last document it
     * read. Reporting {@code true} for an earlier document lets {@code ValuesSourceReaderOperator} hand the reader a document it has
     * already passed, which reads whichever binary block happens to be loaded.
     */
    public void testCanReuseTracksLastReadDoc() throws IOException {
        withIndex(3, (mapperService, ctx) -> {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
            BlockLoader loader = loader(mapperService);
            try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, ctx)) {
                var storedFields = storedFields(mapperService, ctx, loader);

                readDoc(loader, reader, storedFields, 1);

                assertFalse("must not be reused for an earlier doc", reader.canReuse(0));
                assertTrue("may be reused for the doc it just read", reader.canReuse(1));
                assertTrue("may be reused for a later doc", reader.canReuse(2));
            }
        });
    }

    /**
     * The same check across enough documents to span several binary doc values blocks. Reusing the reader here used to read whichever
     * block was still loaded, which threw an {@link ArrayIndexOutOfBoundsException} for a negative index into the block.
     */
    public void testCanReuseAcrossBinaryBlocks() throws IOException {
        // Values are sized so the 512KB binary block threshold flushes every ~256 docs, putting doc 900 and doc 10 in different blocks.
        withIndex(1000, 2048, (mapperService, ctx) -> {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
            BlockLoader loader = loader(mapperService);
            try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, ctx)) {
                readDoc(loader, reader, storedFields(mapperService, ctx, loader), 900);
                assertFalse("must not be reused for a doc in an earlier block", reader.canReuse(10));
            }
        });
    }

    /**
     * Besides the flat estimate, the reader accounts for the bound the doc values report for their private block buffer, which can be
     * as large as the segment's largest block, and gives all of it back on close. Without this, a query that reads many fields holds
     * one such buffer per field while the breaker only sees the flat estimate.
     */
    public void testAccountsForTheBlockBufferBoundAndReleasesItOnClose() throws IOException {
        withIndex(1000, 2048, (mapperService, ctx) -> {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(4));
            BlockLoader loader = loader(mapperService);
            var docValues = ctx.reader().getBinaryDocValues(IgnoredSourceFieldMapper.NAME);
            assertThat(
                "the doc values must reach the TSDB reader for the bound to be known",
                docValues,
                instanceOf(BlockLoader.OptionalDecodeMemoryUsageEstimator.class)
            );
            long bound = ((BlockLoader.OptionalDecodeMemoryUsageEstimator) docValues).maxDecodeBytes();
            assertThat("a block holds at least one value", bound, greaterThan(2048L));

            try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, ctx)) {
                assertThat(breaker.getUsed(), equalTo(BlockSourceReader.ESTIMATED_SIZE + bound));
                readDoc(loader, reader, storedFields(mapperService, ctx, loader), 10);
            }
            assertThat(breaker.getUsed(), equalTo(0L));
        });
    }

    /**
     * A reader that has already opened its doc values is built even if the block bound it then accounts for does not fit in the breaker, so
     * the breaker ends up over its limit and the next reader fails while accounting for its own flat estimate with
     * a {@link CircuitBreakingException}, before any block is decompressed. A query that reads many fields therefore stops after the
     * first reader that goes over, and everything accounted for is given back on close.
     */
    public void testCircuitBreaksTheNextReaderWhenTheBlockBufferBoundExceedsTheBreaker() throws IOException {
        // Each value is 2MB, so the segment's largest block cannot fit in a 1MB breaker.
        long limit = ByteSizeValue.ofMb(1).getBytes();
        withIndex(2, 2 * 1024 * 1024, (mapperService, ctx) -> {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(limit));
            BlockLoader loader = loader(mapperService);
            try (BlockLoader.RowStrideReader first = loader.rowStrideReader(breaker, ctx)) {
                assertThat("the bound is accounted for without breaking", breaker.getUsed(), greaterThan(limit));
                expectThrows(CircuitBreakingException.class, () -> loader.rowStrideReader(breaker, ctx));
            }
            assertThat(breaker.getUsed(), equalTo(0L));
        });
    }

    /**
     * The reader accounts for its flat estimate before it opens the doc values, so a failure to open them must give it back:
     * the constructor never returns, so {@code close} is never called.
     */
    public void testReleasesWhatWasAccountedForWhenOpeningTheDocValuesFails() throws IOException {
        withIndex(3, (mapperService, ctx) -> {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
            BlockLoader loader = loader(mapperService);
            LeafReader failing = new FilterLeafReader(ctx.reader()) {
                @Override
                public BinaryDocValues getBinaryDocValues(String field) throws IOException {
                    throw new IOException("simulated failure to open the doc values");
                }

                @Override
                public CacheHelper getCoreCacheHelper() {
                    return in.getCoreCacheHelper();
                }

                @Override
                public CacheHelper getReaderCacheHelper() {
                    return in.getReaderCacheHelper();
                }
            };
            expectThrows(IOException.class, () -> loader.rowStrideReader(breaker, failing.getContext()));
            assertThat(breaker.getUsed(), equalTo(0L));
        });
    }

    private BlockLoader loader(MapperService mapperService) {
        BlockLoader loader = mapperService.fieldType("field")
            .blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService));
        // Guard against the test silently degrading to a stored field format, which has no per-document reader state.
        assertThat(loader, instanceOf(FallbackSyntheticSourceBlockLoader.class));
        assertThat(
            ((FallbackSyntheticSourceBlockLoader) loader).ignoredSourceFormat(),
            equalTo(IgnoredSourceFieldMapper.IgnoredSourceFormat.DOC_VALUES_IGNORED_SOURCE)
        );
        return loader;
    }

    private static void readDoc(
        BlockLoader loader,
        BlockLoader.RowStrideReader reader,
        BlockLoaderStoredFieldsFromLeafLoader storedFields,
        int doc
    ) throws IOException {
        BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
        storedFields.advanceTo(doc);
        reader.read(doc, storedFields, builder);
        builder.build().close();
    }

    private static BlockLoaderStoredFieldsFromLeafLoader storedFields(
        MapperService mapperService,
        LeafReaderContext ctx,
        BlockLoader loader
    ) throws IOException {
        var sourceLoader = mapperService.mappingLookup()
            .newSourceLoader(new SourceFilter(new String[] { "field" }, null), SourceFieldMetrics.NOOP, null);
        StoredFieldsSpec spec = loader.rowStrideStoredFieldSpec()
            .merge(new StoredFieldsSpec(false, false, sourceLoader.requiredStoredFields()));
        return new BlockLoaderStoredFieldsFromLeafLoader(
            StoredFieldLoader.fromSpec(spec).getLoader(ctx, null),
            sourceLoader.leaf(ctx, null)
        );
    }

    private void withIndex(int numDocs, CheckedBiConsumer<MapperService, LeafReaderContext, IOException> test) throws IOException {
        withIndex(numDocs, 8, test);
    }

    private void withIndex(int numDocs, int valueLength, CheckedBiConsumer<MapperService, LeafReaderContext, IOException> test)
        throws IOException {
        var settings = Settings.builder()
            .put("index.mapping.source.mode", "synthetic")
            // DOC_VALUES_IGNORED_SOURCE requires the TSDB doc values format to be enabled
            .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true)
            .build();
        // A keyword without doc values and without stored fields can only be reconstructed from _ignored_source.
        MapperService mapperService = createMapperService(
            getVersion(),
            settings,
            () -> true,
            mapping(b -> b.startObject("field").field("type", "keyword").field("doc_values", false).field("index", false).endObject())
        );
        withLuceneIndex(mapperService, writer -> {
            for (int i = 0; i < numDocs; i++) {
                String value = Strings.padStart(Integer.toString(i), valueLength, 'v');
                ParsedDocument parsed = mapperService.documentParser()
                    .parseDocument(source(b -> b.field("field", value)), mapperService.mappingLookup());
                writer.addDocuments(parsed.docs());
            }
            // Keep everything in one segment so the reader is exercised across pages of the same segment.
            writer.forceMerge(1);
        }, reader -> {
            assertThat(reader.leaves(), hasSize(1));
            test.accept(mapperService, reader.leaves().get(0));
        });
    }
}
