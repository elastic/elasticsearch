/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchLookupTests extends ESTestCase {
    public void testDocValuesLoop() {
        SearchLookup lookup = new SearchLookup(
            mockMapperService(),
            (ft, l) -> mockIFDNeverReturns(lrc -> l.get().getLeafSearchLookup(lrc).doc().get(ft.name()))
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> collect(lookup, l -> l.doc().get("a")));
        assertThat(e.getMessage(), equalTo("loop in field definitions: [a, a]"));
    }

    public void testDocValuesLooseLoop() {
        SearchLookup lookup = new SearchLookup(mockMapperService(), (ft, l) -> {
            if (ft.name().length() > 3) {
                return mockIFDNeverReturns(lrc -> l.get().forField("a"));
            }
            return mockIFDNeverReturns(lrc -> l.get().getLeafSearchLookup(lrc).doc().get(ft.name() + "a"));
        });
        Exception e = expectThrows(IllegalArgumentException.class, () -> collect(lookup, l -> l.doc().get("a")));
        assertThat(e.getMessage(), equalTo("loop in field definitions: [a, aa, aaa, aaaa, a]"));
    }

    public void testDocValuesTerminatesInLoop() {
        SearchLookup lookup = new SearchLookup(mockMapperService(), (ft, l) -> {
            if (ft.name().length() > 3) {
                return mockIFDNeverReturns(lrc -> l.get().forField(ft.name()));
            }
            return mockIFDNeverReturns(lrc -> l.get().getLeafSearchLookup(lrc).doc().get(ft.name() + "a"));
        });
        Exception e = expectThrows(IllegalArgumentException.class, () -> collect(lookup, l -> l.doc().get("a")));
        assertThat(e.getMessage(), equalTo("loop in field definitions: [a, aa, aaa, aaaa, aaaa]"));
    }

    /**
     * Test a loop that only happens on <strong>some</strong> documents.
     */
    public void testDocValuesSometimesLoop() {
        SearchLookup lookup = new SearchLookup(mockMapperService(), (ft, l) -> {
            if (ft.name().equals("test")) {
                return new SortedNumericIndexFieldData("test", NumericType.LONG);
            }
            if (ft.name().length() > 3) {
                return mockIFDNeverReturns(lrc -> l.get().forField("a"));
            }
            return mockIFD(lrc -> {
                LeafFieldData lfd = mock(LeafFieldData.class);
                when(lfd.getScriptValues()).thenAnswer(inv -> {
                    return new ScriptDocValues<String>() {
                        @Override
                        public void setNextDocId(int docId) throws IOException {
                            LeafSearchLookup lookup = l.get().getLeafSearchLookup(lrc);
                            lookup.setDocument(docId);
                            ScriptDocValues<?> testDv = lookup.doc().get("test");
                            if (testDv.get(0).equals(2L)) {
                                lookup.doc().get(ft.name() + "a");
                            }
                        }

                        @Override
                        public String get(int index) {
                            return null;
                        }

                        @Override
                        public int size() {
                            return 0;
                        }
                    };
                });
                return lfd;
            });
        });
        Exception e = expectThrows(IllegalArgumentException.class, () -> collect(lookup, l -> l.doc().get("a")));
        assertThat(e.getMessage(), equalTo("loop in field definitions: [a, aa, aaa, aaaa, a]"));
    }

    public void testDeepDocValues() throws IOException {
        SearchLookup lookup = new SearchLookup(mockMapperService(), (ft, l) -> {
            if (ft.name().length() > 3) {
                return mockIFD(lrc -> {
                    LeafFieldData lfd = mock(LeafFieldData.class);
                    when(lfd.getScriptValues()).thenAnswer(inv -> {
                        return new ScriptDocValues<String>() {
                            @Override
                            public void setNextDocId(int docId) throws IOException {}

                            @Override
                            public String get(int index) {
                                return "test";
                            }

                            @Override
                            public int size() {
                                return 1;
                            }
                        };
                    });
                    return lfd;
                });
            }
            return mockIFD(lrc -> {
                LeafFieldData lfd = mock(LeafFieldData.class);
                when(lfd.getScriptValues()).thenAnswer(inv -> {
                    return new ScriptDocValues<String>() {
                        String value;

                        @Override
                        public void setNextDocId(int docId) throws IOException {
                            LeafSearchLookup lookup = l.get().getLeafSearchLookup(lrc);
                            lookup.setDocument(docId);
                            ScriptDocValues<?> next = lookup.doc().get(ft.name() + "a");
                            value = next.get(0) + "a";
                        }

                        @Override
                        public String get(int index) {
                            return value;
                        }

                        @Override
                        public int size() {
                            return 1;
                        }
                    };
                });
                return lfd;
            });
        });
        assertThat(collect(lookup, l -> l.doc().get("a")), equalTo(List.of("testaaa", "testaaa")));
    }

    public void testDocValuesTooDeep() {
        SearchLookup lookup = new SearchLookup(
            mockMapperService(),
            (ft, l) -> mockIFDNeverReturns(lrc -> l.get().getLeafSearchLookup(lrc).doc().get(ft.name() + "a"))
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> collect(lookup, l -> l.doc().get("a")));
        assertThat(e.getMessage(), startsWith("field definition too deep: [a, aa, aaa, aaaa,"));
        assertThat(e.getMessage(), endsWith(", aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa]"));
    }

    private IndexFieldData<?> mockIFDNeverReturns(Consumer<LeafReaderContext> load) {
        return mockIFD(lrc -> {
            load.accept(lrc);
            return null;
        });
    }

    private IndexFieldData<?> mockIFD(Function<LeafReaderContext, LeafFieldData> load) {
        IndexFieldData<?> ifd = mock(IndexFieldData.class);
        when(ifd.load(any())).thenAnswer(inv -> load.apply((LeafReaderContext) inv.getArguments()[0]));
        return ifd;
    }

    private MapperService mockMapperService() {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(any())).thenAnswer(inv -> {
            String name = (String) inv.getArguments()[0];
            MappedFieldType ft = mock(MappedFieldType.class);
            when(ft.name()).thenReturn(name);
            return ft;
        });
        return mapperService;
    }

    private List<String> collect(SearchLookup lookup, CheckedFunction<LeafSearchLookup, ScriptDocValues<?>, IOException> getDocValues)
        throws IOException {
        List<String> result = new ArrayList<>();
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            indexWriter.addDocument(List.of(new SortedNumericDocValuesField("test", 1)));
            indexWriter.addDocument(List.of(new SortedNumericDocValuesField("test", 2)));
            try (DirectoryReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                searcher.search(new MatchAllDocsQuery(), new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) throws IOException {}

                            @Override
                            public void collect(int doc) throws IOException {
                                leafLookup.setDocument(doc);
                                ScriptDocValues<?> sdv = getDocValues.apply(leafLookup);
                                sdv.setNextDocId(doc);
                                for (Object v : sdv) {
                                    result.add(v.toString());
                                }
                            }
                        };
                    }
                });
            }
            return result;
        }
    }
}
