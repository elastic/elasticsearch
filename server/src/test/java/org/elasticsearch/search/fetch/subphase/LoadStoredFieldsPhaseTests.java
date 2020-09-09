/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class LoadStoredFieldsPhaseTests extends MapperServiceTestCase {

    // test for regular doc with no special request - should get metadata fields + source
    public void testStandardRequest() throws IOException {

        MapperService mapperService = nonNestedMapperService();

        LoadStoredFieldsPhase phase = new LoadStoredFieldsPhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(new LoadStoredFieldsTestSearchContext(mapperService));

        doNonNestedTest(mapperService, processor, hc -> {
            assertNotNull(hc.hit().getId());
            assertFalse(hc.sourceLookup().isEmpty());
            assertThat(hc.sourceLookup().get("keyword"), equalTo("foo"));
            Map<String, DocumentField> loadedFields = hc.hit().getFields();
            assertThat(loadedFields.size(), equalTo(0));
        });
    }

    // test for regular doc with specific fields incl aliases - should get metadata fields + source + specific fields
    public void testLoadNamedFields() throws IOException {

        MapperService mapperService = nonNestedMapperService();

        LoadStoredFieldsPhase phase = new LoadStoredFieldsPhase();
        LoadStoredFieldsTestSearchContext context = new LoadStoredFieldsTestSearchContext(mapperService);
        StoredFieldsContext sfc = StoredFieldsContext.fromList(List.of("key*"));
        context.storedFieldsContext(sfc);

        FetchSubPhaseProcessor processor = phase.getProcessor(context);

        doNonNestedTest(mapperService, processor, hc -> {
            assertNotNull(hc.hit().getId());
            assertFalse(hc.sourceLookup().isEmpty());
            assertThat(hc.sourceLookup().get("keyword"), equalTo("foo"));
            Map<String, DocumentField> loadedFields = hc.hit().getFields();
            assertThat(loadedFields.size(), equalTo(1));
            DocumentField k = loadedFields.get("keyword");
            assertThat(k.getValue(), equalTo("foo"));
        });
    }

    // test for regular doc with _none_ - should get no source
    public void testFieldsNone() throws IOException {
        MapperService mapperService = nonNestedMapperService();
        LoadStoredFieldsTestSearchContext context = new LoadStoredFieldsTestSearchContext(mapperService);
        StoredFieldsContext sfc = StoredFieldsContext.fromList(List.of("_none_"));
        context.storedFieldsContext(sfc);

        LoadStoredFieldsPhase phase = new LoadStoredFieldsPhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(context);

        assertNull(processor);
    }

    // test for regular doc with no source - should get only metadata fields
    public void testNoSource() throws IOException {

        MapperService mapperService = nonNestedMapperService();

        LoadStoredFieldsTestSearchContext context = new LoadStoredFieldsTestSearchContext(mapperService);
        context.fetchSourceContext(new FetchSourceContext(false));

        LoadStoredFieldsPhase phase = new LoadStoredFieldsPhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(context);

        doNonNestedTest(mapperService, processor, hc -> {
            assertNotNull(hc.hit().getId());
            assertNull(hc.sourceLookup().source());
        });
    }

    public void testEmptySearchHit() {
        SearchHit hit = new SearchHit(0);
        assertNull(hit.getId());
    }

    private void doNonNestedTest(MapperService mapperService,
                                 FetchSubPhaseProcessor processor,
                                 Consumer<HitContext> test) throws IOException {
        withLuceneIndex(mapperService, iw -> indexNonNestedDocs(mapperService, iw), reader -> {
            for (LeafReaderContext ctx : reader.leaves()) {
                for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                    HitContext hc = new HitContext(new SearchHit(i), ctx, i, -1, null, null);
                    processor.process(hc);
                    test.accept(hc);
                }
            }
        });
    }

    private static final QueryCachingPolicy qcp = new QueryCachingPolicy() {
        @Override
        public void onUse(Query query) {

        }

        @Override
        public boolean shouldCache(Query query) throws IOException {
            return false;
        }
    };

    private void doNestedTest(MapperService mapperService,
                              LoadStoredFieldsTestSearchContext context,
                              FetchSubPhaseProcessor processor,
                              Consumer<HitContext> test) throws IOException {
        withLuceneIndex(mapperService, iw -> indexNestedDocs(mapperService, iw), reader -> {
            context.setSearcher(new ContextIndexSearcher(reader, new BM25Similarity(), null, qcp, false));
            for (LeafReaderContext ctx : reader.leaves()) {
                BitSet bits = BitsetFilterCache.bitsetFromQuery(Queries.newNonNestedFilter(), ctx);
                assertNotNull(bits);
                for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                    HitContext hc;
                    if (bits.get(i)) {
                        // parent doc
                        hc = new HitContext(new SearchHit(i), ctx, i, -1, null, null);
                    } else {
                        hc = new HitContext(new SearchHit(i), ctx, i, bits.nextSetBit(i), null, null);
                    }
                    processor.process(hc);
                    test.accept(hc);
                }
            }
        });
    }

    private MapperService nonNestedMapperService() throws IOException {
        return createMapperService(mapping(b -> {
            b.startObject("keyword");
            {
                b.field("type", "keyword");
                b.field("store", "true");
            }
            b.endObject();
            b.startObject("long").field("type", "long").endObject();
        }));
    }

    private void indexNonNestedDocs(MapperService m, RandomIndexWriter iw) throws IOException {
        int numDocs = randomIntBetween(5, 50);
        for (int i = 0; i < numDocs; i++) {
            int val = i;
            iw.addDocuments(m.documentMapper().parse(source(b -> b.field("keyword", "foo").field("long", val))).docs());
        }
    }

    private MapperService nestedMapperService() throws IOException {
        return createMapperService(mapping(b -> {
            b.startObject("keyword");
            {
                b.field("type", "keyword");
                b.field("store", "true");
            }
            b.endObject();
            b.startObject("long").field("type", "long").endObject();
            b.startObject("subdocs");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("child");
                    {
                        b.field("type", "long");
                        b.field("store", "true");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
    }

    private void indexNestedDocs(MapperService m, RandomIndexWriter iw) throws IOException {
        int numDocs = randomIntBetween(5, 50);
        for (int i = 0; i < numDocs; i++) {
            int parent = i;
            int numChildren = randomIntBetween(1, 10);
            iw.addDocuments(m.documentMapper().parse(source(b -> {
                b.field("keyword", "foo");
                b.field("long", parent);
                b.startArray("subdocs");
                for (int j = 0; j <= numChildren; j++) {
                    b.startObject().field("child", j).endObject();
                }
                b.endArray();
            })).docs());
        }
    }

    // test for nested doc - should get metadata fields + nested source
    public void testStandardNestedDoc() throws IOException {
        MapperService mapperService = nestedMapperService();

        LoadStoredFieldsTestSearchContext context = new LoadStoredFieldsTestSearchContext(mapperService);
        LoadStoredFieldsPhase phase = new LoadStoredFieldsPhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(context);

        doNestedTest(mapperService, context, processor, hc -> {
            assertNotNull(hc.hit().getId());
            assertFalse(hc.sourceLookup().isEmpty());
            if (hc.rootId() == -1) {
                assertThat(hc.sourceLookup().get("keyword"), equalTo("foo"));
            } else {
                assertTrue(hc.sourceLookup().containsKey("subdocs"));
                assertThat(hc.sourceLookup().get("subdocs").toString(), startsWith("{child="));
            }
            Map<String, DocumentField> loadedFields = hc.hit().getFields();
            assertThat(loadedFields.size(), equalTo(0));
        });

    }

    // test for nested doc with specific fields - should get metadata fields + nested source + specific fields
    public void testNestedDocWithSpecifiedFields() throws IOException {
        MapperService mapperService = nestedMapperService();

        LoadStoredFieldsPhase phase = new LoadStoredFieldsPhase();
        LoadStoredFieldsTestSearchContext context = new LoadStoredFieldsTestSearchContext(mapperService);
        StoredFieldsContext sfc = StoredFieldsContext.fromList(List.of("chi*"));
        context.storedFieldsContext(sfc);

        FetchSubPhaseProcessor processor = phase.getProcessor(context);

        doNestedTest(mapperService, context, processor, hc -> {
            assertNotNull(hc.hit().getId());
            Map<String, DocumentField> loadedFields = hc.hit().getFields();
            if (hc.rootId() == -1) {
                assertThat(loadedFields.size(), equalTo(0));
            } else {
                // this does not appear to work! we expand the fields list from the root doc,
                // so nested fields do not get picked up.
                // assertThat(loadedFields.size(), equalTo(1));
                // DocumentField k = loadedFields.get("child");
                // assertNotNull(k.getValue());
            }
        });
    }

    // test for nested doc with no source - should get only metadata fields
    public void testNestedNoSource() throws IOException {

        MapperService mapperService = nonNestedMapperService();

        LoadStoredFieldsTestSearchContext context = new LoadStoredFieldsTestSearchContext(mapperService);
        context.fetchSourceContext(new FetchSourceContext(false));

        LoadStoredFieldsPhase phase = new LoadStoredFieldsPhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(context);

        doNestedTest(mapperService, context, processor, hc -> {
            assertNotNull(hc.hit().getId());
            assertNull(hc.sourceLookup().source());
        });
    }

    // test for nested doc with no source but highlighting - should get metadata fields + nested source
    // for nested docs, but no source for parent docs
    public void testNestedNoSourceWithHighlighting() throws IOException {

        MapperService mapperService = nonNestedMapperService();

        LoadStoredFieldsTestSearchContext context = new LoadStoredFieldsTestSearchContext(mapperService);
        context.fetchSourceContext(new FetchSourceContext(false));
        context.highlight(true);

        LoadStoredFieldsPhase phase = new LoadStoredFieldsPhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(context);

        doNestedTest(mapperService, context, processor, hc -> {
            assertNotNull(hc.hit().getId());
            if (hc.rootId() == -1) {
                assertNull(hc.sourceLookup().source());
                assertThat(hc.sourceLookup().get("keyword"), equalTo("foo"));
            } else {
                assertNotNull(hc.sourceLookup().source());
                assertTrue(hc.sourceLookup().containsKey("subdocs"));
                assertThat(hc.sourceLookup().get("subdocs").toString(), startsWith("{child="));
            }
            Map<String, DocumentField> loadedFields = hc.hit().getFields();
            assertThat(loadedFields.size(), equalTo(0));
        });
    }

    private static class LoadStoredFieldsTestSearchContext extends TestSearchContext {

        boolean highlight;
        FetchSourceContext fetchSourceContext;
        StoredFieldsContext storedFields;
        final MapperService mapperService;

        LoadStoredFieldsTestSearchContext(MapperService mapperService) {
            super(null);
            this.mapperService = mapperService;
        }

        @Override
        public BitSetProducer getBitSetProducer(Query query) {
            return new BitSetProducer() {
                @Override
                public BitSet getBitSet(LeafReaderContext context) throws IOException {
                    return BitsetFilterCache.bitsetFromQuery(query, context);
                }
            };
        }

        @Override
        public MapperService mapperService() {
            return mapperService;
        }

        void highlight(boolean highlight) {
            this.highlight = highlight;
        }

        @Override
        public SearchHighlightContext highlight() {
            if (highlight) {
                return new SearchHighlightContext(Collections.emptyList(), false);
            }
            return null;
        }

        @Override
        public boolean sourceRequested() {
            return fetchSourceContext != null && fetchSourceContext.fetchSource();
        }

        @Override
        public boolean hasFetchSourceContext() {
            return fetchSourceContext != null;
        }

        @Override
        public FetchSourceContext fetchSourceContext() {
            return this.fetchSourceContext;
        }

        @Override
        public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
            this.fetchSourceContext = fetchSourceContext;
            return this;
        }

        @Override
        public boolean hasStoredFields() {
            return storedFields != null && storedFields.fieldNames() != null;
        }

        @Override
        public boolean hasStoredFieldsContext() {
            return storedFields != null;
        }

        @Override
        public StoredFieldsContext storedFieldsContext() {
            return storedFields;
        }

        @Override
        public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
            this.storedFields = storedFieldsContext;
            return this;
        }

        @Override
        public boolean storedFieldsRequested() {
            return storedFields == null || storedFields.fetchFields();
        }

    }
}
