/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMetrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

public class ConcurrentSegmentSourceProviderTests extends MapperServiceTestCase {

    public void testReusesLeafOfShortLivedReaderForTheSameDoc() throws IOException {
        for (boolean synthetic : new boolean[] { true, false }) {
            withIndex(synthetic, 1, (provider, segment) -> {
                LeafReaderContext ctx = shortLivedContext(segment.reader());
                Source source = provider.getSource(ctx, 0);
                assertSame(source, provider.getSource(ctx, 0));
            });
        }
    }

    public void testDoesNotRetainLeavesOfShortLivedReaders() throws IOException {
        for (boolean synthetic : new boolean[] { true, false }) {
            withIndex(synthetic, 1, (provider, segment) -> {
                int numReaders = between(2, 50);
                List<LeafReaderContext> contexts = new ArrayList<>(numReaders);
                List<Source> sources = new ArrayList<>(numReaders);
                for (int i = 0; i < numReaders; i++) {
                    LeafReaderContext ctx = shortLivedContext(segment.reader());
                    contexts.add(ctx);
                    sources.add(provider.getSource(ctx, 0));
                }
                // Only the most recent short-lived reader may still have its leaf, every earlier one has to start over
                for (int i = 0; i < numReaders - 1; i++) {
                    assertNotSame(
                        "leaf of short-lived reader [" + i + "] was retained",
                        sources.get(i),
                        provider.getSource(contexts.get(i), 0)
                    );
                }
            });
        }
    }

    public void testKeepsLeafOfSegmentReader() throws IOException {
        for (boolean synthetic : new boolean[] { true, false }) {
            withIndex(synthetic, 1, (provider, segment) -> {
                assertNotNull(segment.reader().getCoreCacheHelper());
                Source source = provider.getSource(segment, 0);
                provider.getSource(shortLivedContext(segment.reader()), 0);
                assertSame(source, provider.getSource(segment, 0));
            });
        }
    }

    public void testShortLivedReaderReadsLowerDocAfterHigherDoc() throws IOException {
        for (boolean synthetic : new boolean[] { true, false }) {
            withIndex(synthetic, 2, (provider, segment) -> {
                LeafReaderContext ctx = shortLivedContext(segment.reader());
                assertThat(provider.getSource(ctx, 1).source(), equalTo(Map.of("field", "value-1")));
                assertThat(provider.getSource(ctx, 0).source(), equalTo(Map.of("field", "value-0")));
            });
        }
    }

    public void testConcurrentShortLivedReadersReadTheRightDocs() throws IOException {
        for (boolean synthetic : new boolean[] { true, false }) {
            withIndex(synthetic, between(10, 50), (provider, segment) -> {
                int numDocs = segment.reader().maxDoc();
                List<Map<String, Object>> expected = new ArrayList<>(numDocs);
                for (int doc = 0; doc < numDocs; doc++) {
                    expected.add(provider.getSource(segment, doc).source());
                }
                int numThreads = between(2, 8);
                long[] seeds = new long[numThreads];
                for (int t = 0; t < numThreads; t++) {
                    seeds[t] = randomLong();
                }
                // Each thread uses its own short-lived readers, so the threads keep replacing each other's cached leaf
                startInParallel(numThreads, t -> {
                    Random random = new Random(seeds[t]);
                    try {
                        for (int i = 0; i < 20; i++) {
                            LeafReaderContext ctx = shortLivedContext(segment.reader());
                            for (int doc = random.nextInt(numDocs); doc < numDocs; doc += 1 + random.nextInt(5)) {
                                assertThat(provider.getSource(ctx, doc).source(), equalTo(expected.get(doc)));
                                assertThat(provider.getSource(ctx, doc).source(), equalTo(expected.get(doc)));
                            }
                            int lowerDoc = random.nextInt(numDocs);
                            assertThat(provider.getSource(ctx, lowerDoc).source(), equalTo(expected.get(lowerDoc)));
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            });
        }
    }

    private void withIndex(boolean synthetic, int numDocs, CheckedBiConsumer<SourceProvider, LeafReaderContext, IOException> test)
        throws IOException {
        var mapping = mapping(b -> b.startObject("field").field("type", "keyword").endObject());
        MapperService mapperService = synthetic ? createSytheticSourceMapperService(mapping) : createMapperService(mapping);
        DocumentMapper mapper = mapperService.documentMapper();
        withLuceneIndex(mapperService, iw -> {
            for (int i = 0; i < numDocs; i++) {
                String value = "value-" + i;
                ParsedDocument doc = mapper.parse(source(b -> b.field("field", value)));
                doc.updateSeqID(i, 0);
                doc.version().setLongValue(0);
                iw.addDocuments(doc.docs());
            }
            iw.forceMerge(1);
        }, reader -> {
            SourceProvider provider = SourceProvider.fromLookup(mapper.mappers(), null, SourceFieldMetrics.NOOP);
            test.accept(provider, getOnlyLeafReader(reader).getContext());
        });
    }

    /**
     * A fresh, non-cacheable context over the segment, like the per-document readers the unified highlighter builds.
     */
    private static LeafReaderContext shortLivedContext(LeafReader segment) {
        LeafReader wrapper = new FilterLeafReader(segment) {
            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
        return wrapper.getContext();
    }
}
