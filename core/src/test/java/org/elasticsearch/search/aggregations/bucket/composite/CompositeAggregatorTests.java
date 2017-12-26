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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.IndexSettingsModule;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CompositeAggregatorTests extends AggregatorTestCase {
    private static MappedFieldType[] FIELD_TYPES;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        FIELD_TYPES = new MappedFieldType[5];
        FIELD_TYPES[0] = new KeywordFieldMapper.KeywordFieldType();
        FIELD_TYPES[0].setName("keyword");
        FIELD_TYPES[0].setHasDocValues(true);

        FIELD_TYPES[1] = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        FIELD_TYPES[1].setName("long");
        FIELD_TYPES[1].setHasDocValues(true);

        FIELD_TYPES[2] = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        FIELD_TYPES[2].setName("double");
        FIELD_TYPES[2].setHasDocValues(true);

        DateFieldMapper.Builder builder = new DateFieldMapper.Builder("date");
        builder.docValues(true);
        DateFieldMapper fieldMapper =
            builder.build(new Mapper.BuilderContext(createIndexSettings().getSettings(), new ContentPath(0)));
        FIELD_TYPES[3] = fieldMapper.fieldType();

        FIELD_TYPES[4] = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        FIELD_TYPES[4].setName("price");
        FIELD_TYPES[4].setHasDocValues(true);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        FIELD_TYPES = null;
    }

    public void testWithKeyword() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "a"),
                createDocument("keyword", "c"),
                createDocument("keyword", "a"),
                createDocument("keyword", "d"),
                createDocument("keyword", "c")
            )
        );
        final Sort sort = new Sort(new SortedSetSortField("keyword", false));
        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "a"));
            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=c}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
            }
        );
    }

    public void testWithKeywordMissingAfter() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "foo"),
                createDocument("keyword", "bar"),
                createDocument("keyword", "foo"),
                createDocument("keyword", "zoo"),
                createDocument("keyword", "bar"),
                createDocument("keyword", "delta")
            )
        );
        final Sort sort = new Sort(new SortedSetSortField("keyword", false));
        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=bar}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=delta}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=foo}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=zoo}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "car"));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=delta}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=foo}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=zoo}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword").order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "mar"));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=foo}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=delta}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=bar}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        );
    }

   public void testWithKeywordDesc() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "a"),
                createDocument("keyword", "c"),
                createDocument("keyword", "a"),
                createDocument("keyword", "d"),
                createDocument("keyword", "c")
            )
        );
        final Sort sort = new Sort(new SortedSetSortField("keyword", true));
        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=a}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "c"));

            }, (result) -> {
                assertEquals(1, result.getBuckets().size());
                assertEquals("{keyword=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
            }
        );
    }

    public void testMultiValuedWithKeyword() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", Arrays.asList("a", "b")),
                createDocument("keyword", Arrays.asList("c", "a")),
                createDocument("keyword", Arrays.asList("b", "d")),
                createDocument("keyword", Arrays.asList("z")),
                createDocument("keyword", Collections.emptyList())
            )
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));

            }, (result) -> {
                    assertEquals(5, result.getBuckets().size());
                    assertEquals("{keyword=a}", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(2L, result.getBuckets().get(0).getDocCount());
                    assertEquals("{keyword=b}", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(2L, result.getBuckets().get(1).getDocCount());
                    assertEquals("{keyword=c}", result.getBuckets().get(2).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    assertEquals("{keyword=d}", result.getBuckets().get(3).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(3).getDocCount());
                    assertEquals("{keyword=z}", result.getBuckets().get(4).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(4).getDocCount());
                }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "b"));

            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=c}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=z}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );
    }

    public void testMultiValuedWithKeywordDesc() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", Arrays.asList("a", "b")),
                createDocument("keyword", Arrays.asList("c", "a")),
                createDocument("keyword", Arrays.asList("b", "d")),
                createDocument("keyword", Arrays.asList("z")),
                createDocument("keyword", Collections.emptyList())
            )
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));

            }, (result) -> {
                assertEquals(5, result.getBuckets().size());
                assertEquals("{keyword=a}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=b}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=c}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=z}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "c"));

            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=a}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=b}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
            }
        );
    }

    public void testWithKeywordAndLong() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "a", "long", 100L),
                createDocument("keyword", "c", "long", 100L),
                createDocument("keyword", "a", "long", 0L),
                createDocument("keyword", "d", "long", 10L),
                createDocument("keyword", "c"),
                createDocument("keyword", "c", "long", 100L),
                createDocument("long", 100L)
            )
        );
        final Sort sort = new Sort(
            new SortedSetSortField("keyword", false),
            new SortedNumericSortField("long", SortField.Type.LONG)
        );
        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    )
            ),
            (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=a, long=0}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=d, long=10}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    )
                ).aggregateAfter(createAfterKey("keyword", "a", "long", 100L)
            ),
            (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=d, long=10}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
            }
        );
    }

    public void testWithKeywordAndLongDesc() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "a", "long", 100L),
                createDocument("keyword", "c", "long", 100L),
                createDocument("keyword", "a", "long", 0L),
                createDocument("keyword", "d", "long", 10L),
                createDocument("keyword", "c"),
                createDocument("keyword", "c", "long", 100L),
                createDocument("long", 100L)
            )
        );
        final Sort sort = new Sort(
            new SortedSetSortField("keyword", true),
            new SortedNumericSortField("long", SortField.Type.LONG, true)
        );
        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword").order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("long").field("long").order(SortOrder.DESC)
                    )
                ),
            (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=a, long=0}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d, long=10}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword").order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("long").field("long").order(SortOrder.DESC)
                    )).aggregateAfter(createAfterKey("keyword", "d", "long", 10L)
                ), (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=a, long=0}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
            }
        );
    }

    public void testMultiValuedWithKeywordAndLong() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", Arrays.asList("a", "b", "c"), "long", 100L),
                createDocument("keyword", "c", "long", Arrays.asList(100L, 0L, 10L)),
                createDocument("keyword", Arrays.asList("a", "z"), "long", Arrays.asList(0L, 100L)),
                createDocument("keyword", Arrays.asList("d", "d"), "long", Arrays.asList(10L, 100L, 1000L)),
                createDocument("keyword", "c"),
                createDocument("long", 100L)
            )
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    ))
            , (result) -> {
                assertEquals(10, result.getBuckets().size());
                assertEquals("{keyword=a, long=0}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=b, long=100}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=c, long=0}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=c, long=10}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(5).getDocCount());
                assertEquals("{keyword=d, long=10}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(6).getDocCount());
                assertEquals("{keyword=d, long=100}", result.getBuckets().get(7).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(7).getDocCount());
                assertEquals("{keyword=d, long=1000}", result.getBuckets().get(8).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(8).getDocCount());
                assertEquals("{keyword=z, long=0}", result.getBuckets().get(9).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(9).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    )
                ).aggregateAfter(createAfterKey("keyword", "c", "long", 10L))
            , (result) -> {
                assertEquals(6, result.getBuckets().size());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=d, long=10}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d, long=100}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=d, long=1000}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=z, long=0}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=z, long=100}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(5).getDocCount());
            }
        );
    }

    public void testMultiValuedWithKeywordAndLongDesc() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", Arrays.asList("a", "b", "c"), "long", 100L),
                createDocument("keyword", "c", "long", Arrays.asList(100L, 0L, 10L)),
                createDocument("keyword", Arrays.asList("a", "z"), "long", Arrays.asList(0L, 100L)),
                createDocument("keyword", Arrays.asList("d", "d"), "long", Arrays.asList(10L, 100L, 1000L)),
                createDocument("keyword", "c"),
                createDocument("long", 100L)

            )
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword").order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("long").field("long").order(SortOrder.DESC)
                    )
                ).aggregateAfter(createAfterKey("keyword", "z", "long", 100L)
                ),
            (result) -> {
                assertEquals(10, result.getBuckets().size());
                assertEquals("{keyword=a, long=0}", result.getBuckets().get(9).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(9).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(8).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(8).getDocCount());
                assertEquals("{keyword=b, long=100}", result.getBuckets().get(7).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(7).getDocCount());
                assertEquals("{keyword=c, long=0}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(6).getDocCount());
                assertEquals("{keyword=c, long=10}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(5).getDocCount());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=d, long=10}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=d, long=100}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=d, long=1000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=z, long=0}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword").order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("long").field("long").order(SortOrder.DESC)
                    )
                ).aggregateAfter(createAfterKey("keyword", "b", "long", 100L)
                ),
            (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=a, long=0}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
            }
        );
    }

    public void testMultiValuedWithKeywordLongAndDouble() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", Arrays.asList("a", "b", "c"), "long", 100L, "double", 0.4d),
                createDocument("keyword", "c", "long", Arrays.asList(100L, 0L, 10L), "double", 0.09d),
                createDocument("keyword", Arrays.asList("a", "z", "c"), "long", Arrays.asList(0L, 100L),
                    "double", Arrays.asList(0.4d, 0.09d)),
                createDocument("keyword", Arrays.asList("d", "d"), "long", Arrays.asList(10L, 100L, 1000L), "double", 1.0d),
                createDocument("keyword", "c"),
                createDocument("long", 100L)
            )
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long"),
                        new TermsValuesSourceBuilder("double").field("double")
                    )
                )
            , (result) -> {
                assertEquals(10, result.getBuckets().size());
                assertEquals("{keyword=a, long=0, double=0.09}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=a, long=0, double=0.4}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=a, long=100, double=0.09}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=a, long=100, double=0.4}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=b, long=100, double=0.4}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=c, long=0, double=0.09}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(5).getDocCount());
                assertEquals("{keyword=c, long=0, double=0.4}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(6).getDocCount());
                assertEquals("{keyword=c, long=10, double=0.09}", result.getBuckets().get(7).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(7).getDocCount());
                assertEquals("{keyword=c, long=100, double=0.09}", result.getBuckets().get(8).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(9).getDocCount());
                assertEquals("{keyword=c, long=100, double=0.4}", result.getBuckets().get(9).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(9).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long"),
                        new TermsValuesSourceBuilder("double").field("double")
                    )
                ).aggregateAfter(createAfterKey("keyword", "a", "long", 100L, "double", 0.4d))
            ,(result) -> {
                assertEquals(10, result.getBuckets().size());
                assertEquals("{keyword=b, long=100, double=0.4}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=c, long=0, double=0.09}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=c, long=0, double=0.4}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=c, long=10, double=0.09}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=c, long=100, double=0.09}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=c, long=100, double=0.4}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(5).getDocCount());
                assertEquals("{keyword=d, long=10, double=1.0}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(6).getDocCount());
                assertEquals("{keyword=d, long=100, double=1.0}", result.getBuckets().get(7).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(7).getDocCount());
                assertEquals("{keyword=d, long=1000, double=1.0}", result.getBuckets().get(8).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(8).getDocCount());
                assertEquals("{keyword=z, long=0, double=0.09}", result.getBuckets().get(9).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(9).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long"),
                        new TermsValuesSourceBuilder("long").field("double")
                    )
                ).aggregateAfter(createAfterKey("keyword", "z", "long", 100L, "double", 0.4d))
            , (result) -> {
                assertEquals(0, result.getBuckets().size());
            }
        );
    }

    public void testWithDateHistogram() throws IOException {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("date", asLong("2017-10-20T03:08:45")),
                createDocument("date", asLong("2016-09-20T09:00:34")),
                createDocument("date", asLong("2016-09-20T11:34:00")),
                createDocument("date", asLong("2017-10-20T06:09:24")),
                createDocument("date", asLong("2017-10-19T06:09:24")),
                createDocument("long", 4L)
            )
        );
        final Sort sort = new Sort(new SortedNumericSortField("date", SortField.Type.LONG));
        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1));
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo));
            },
            (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{date=1474329600000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508371200000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=1508457600000}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1));
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo))
                    .aggregateAfter(createAfterKey("date", 1474329600000L));

            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{date=1508371200000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508457600000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
            }
        );
    }

    public void testWithDateHistogramAndTimeZone() throws IOException {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("date", asLong("2017-10-20T03:08:45")),
                createDocument("date", asLong("2016-09-20T09:00:34")),
                createDocument("date", asLong("2016-09-20T11:34:00")),
                createDocument("date", asLong("2017-10-20T06:09:24")),
                createDocument("date", asLong("2017-10-19T06:09:24")),
                createDocument("long", 4L)
            )
        );
        final Sort sort = new Sort(new SortedNumericSortField("date", SortField.Type.LONG));
        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1))
                    .timeZone(DateTimeZone.forOffsetHours(1));
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo));
            },
            (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{date=1474326000000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508367600000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=1508454000000}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), sort, dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1))
                    .timeZone(DateTimeZone.forOffsetHours(1));
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo))
                    .aggregateAfter(createAfterKey("date", 1474326000000L));

            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{date=1508367600000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508454000000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
            }
        );
    }

    public void testWithDateHistogramAndKeyword() throws IOException {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("date", asLong("2017-10-20T03:08:45"), "keyword", Arrays.asList("a", "c")),
                createDocument("date", asLong("2016-09-20T09:00:34"), "keyword", Arrays.asList("b", "c")),
                createDocument("date", asLong("2016-09-20T11:34:00"), "keyword", Arrays.asList("b", "z")),
                createDocument("date", asLong("2017-10-20T06:09:24"), "keyword", Arrays.asList("a", "d")),
                createDocument("date", asLong("2017-10-19T06:09:24"), "keyword", Arrays.asList("g")),
                createDocument("long", 4L)
            )
        );
        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new DateHistogramValuesSourceBuilder("date")
                            .field("date")
                            .dateHistogramInterval(DateHistogramInterval.days(1)),
                        new TermsValuesSourceBuilder("keyword")
                            .field("keyword")
                    )
                ),
            (result) -> {
                assertEquals(7, result.getBuckets().size());
                assertEquals("{date=1474329600000, keyword=b}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1474329600000, keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=1474329600000, keyword=z}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{date=1508371200000, keyword=g}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{date=1508457600000, keyword=a}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(4).getDocCount());
                assertEquals("{date=1508457600000, keyword=c}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(5).getDocCount());
                assertEquals("{date=1508457600000, keyword=d}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(6).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new DateHistogramValuesSourceBuilder("date")
                            .field("date")
                            .dateHistogramInterval(DateHistogramInterval.days(1)),
                        new TermsValuesSourceBuilder("keyword")
                            .field("keyword")
                    )
                ).aggregateAfter(createAfterKey("date", 1508371200000L, "keyword", "g"))
            , (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{date=1508457600000, keyword=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508457600000, keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=1508457600000, keyword=d}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );
    }

    public void testWithKeywordAndHistogram() throws IOException {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("price", 103L, "keyword", Arrays.asList("a", "c")),
                createDocument("price", 51L, "keyword", Arrays.asList("b", "c")),
                createDocument("price", 56L, "keyword", Arrays.asList("b", "z")),
                createDocument("price", 105L, "keyword", Arrays.asList("a", "d")),
                createDocument("price", 25L, "keyword", Arrays.asList("g")),
                createDocument("long", 4L)
            )
        );
        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new HistogramValuesSourceBuilder("price").field("price").interval(10)
                    )
                )
            , (result) -> {
                assertEquals(7, result.getBuckets().size());
                assertEquals("{keyword=a, price=100.0}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=b, price=50.0}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=c, price=50.0}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=c, price=100.0}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=d, price=100.0}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=g, price=20.0}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(1, result.getBuckets().get(5).getDocCount());
                assertEquals("{keyword=z, price=50.0}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(6).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new HistogramValuesSourceBuilder("price").field("price").interval(10)
                    )
                ).aggregateAfter(createAfterKey("keyword", "c", "price", 50.0))
            , (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=c, price=100.0}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=d, price=100.0}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=g, price=20.0}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=z, price=50.0}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
            }
        );
    }

    public void testWithHistogramAndKeyword() throws IOException {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("double", 0.4d, "keyword", Arrays.asList("a", "c")),
                createDocument("double", 0.45d, "keyword", Arrays.asList("b", "c")),
                createDocument("double", 0.8d, "keyword", Arrays.asList("b", "z")),
                createDocument("double", 0.98d, "keyword", Arrays.asList("a", "d")),
                createDocument("double", 0.55d, "keyword", Arrays.asList("g")),
                createDocument("double", 0.4d, "keyword", Arrays.asList("a", "c")),
                createDocument("double", 0.45d, "keyword", Arrays.asList("b", "c")),
                createDocument("double", 0.8d, "keyword", Arrays.asList("b", "z")),
                createDocument("double", 0.98d, "keyword", Arrays.asList("a", "d")),
                createDocument("double", 0.55d, "keyword", Arrays.asList("g")),
                createDocument("long", 4L)
            )
        );
        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new HistogramValuesSourceBuilder("histo").field("double").interval(0.1),
                        new TermsValuesSourceBuilder("keyword").field("keyword")
                    )
                )
            , (result) -> {
                assertEquals(8, result.getBuckets().size());
                assertEquals("{histo=0.4, keyword=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{histo=0.4, keyword=b}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{histo=0.4, keyword=c}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(4L, result.getBuckets().get(2).getDocCount());
                assertEquals("{histo=0.5, keyword=g}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(3).getDocCount());
                assertEquals("{histo=0.8, keyword=b}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(4).getDocCount());
                assertEquals("{histo=0.8, keyword=z}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(5).getDocCount());
                assertEquals("{histo=0.9, keyword=a}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(6).getDocCount());
                assertEquals("{histo=0.9, keyword=d}", result.getBuckets().get(7).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(7).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new HistogramValuesSourceBuilder("histo").field("double").interval(0.1),
                        new TermsValuesSourceBuilder("keyword").field("keyword")
                    )
                ).aggregateAfter(createAfterKey("histo", 0.8d, "keyword", "b"))
            , (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{histo=0.8, keyword=z}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{histo=0.9, keyword=a}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{histo=0.9, keyword=d}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        );
    }

    public void testWithKeywordAndDateHistogram() throws IOException {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("date", asLong("2017-10-20T03:08:45"), "keyword", Arrays.asList("a", "c")),
                createDocument("date", asLong("2016-09-20T09:00:34"), "keyword", Arrays.asList("b", "c")),
                createDocument("date", asLong("2016-09-20T11:34:00"), "keyword", Arrays.asList("b", "z")),
                createDocument("date", asLong("2017-10-20T06:09:24"), "keyword", Arrays.asList("a", "d")),
                createDocument("date", asLong("2017-10-19T06:09:24"), "keyword", Arrays.asList("g")),
                createDocument("long", 4L)
            )
        );
        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new DateHistogramValuesSourceBuilder("date_histo").field("date")
                            .dateHistogramInterval(DateHistogramInterval.days(1))
                    )
                )
            , (result) -> {
                assertEquals(7, result.getBuckets().size());
                assertEquals("{keyword=a, date_histo=1508457600000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=b, date_histo=1474329600000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=c, date_histo=1474329600000}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=c, date_histo=1508457600000}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=d, date_histo=1508457600000}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=g, date_histo=1508371200000}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(5).getDocCount());
                assertEquals("{keyword=z, date_histo=1474329600000}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(6).getDocCount());
            }
        );

        testSearchCase(new MatchAllDocsQuery(), null, dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new DateHistogramValuesSourceBuilder("date_histo").field("date")
                            .dateHistogramInterval(DateHistogramInterval.days(1))
                    )
                ).aggregateAfter(createAfterKey("keyword","c", "date_histo", 1474329600000L))
            , (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=c, date_histo=1508457600000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=d, date_histo=1508457600000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=g, date_histo=1508371200000}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=z, date_histo=1474329600000}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
            }
        );
    }

    private void testSearchCase(Query query,
                                Sort sort,
                                List<Map<String, List<Object>>> dataset,
                                Supplier<CompositeAggregationBuilder> create,
                                Consumer<InternalComposite> verify) throws IOException {
        executeTestCase(false, null, query, dataset, create, verify);
        executeTestCase(true, null, query, dataset, create, verify);
        if (sort != null) {
            executeTestCase(false, sort, query, dataset, create, verify);
            executeTestCase(true, sort, query, dataset, create, verify);
        }
    }

    private void executeTestCase(boolean reduced,
                                 Sort sort,
                                 Query query,
                                 List<Map<String, List<Object>>> dataset,
                                 Supplier<CompositeAggregationBuilder> create,
                                 Consumer<InternalComposite> verify) throws IOException {
        IndexSettings indexSettings = createIndexSettings(sort);
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
            if (sort != null) {
                config.setIndexSort(sort);
                /**
                 * Forces the default codec because {@link CompositeValuesSourceBuilder#checkCanEarlyTerminate}
                 * cannot detect single-valued field with the asserting-codec.
                 **/
                config.setCodec(TestUtil.getDefaultCodec());
            }
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config)) {
                Document document = new Document();
                for (Map<String, List<Object>> fields : dataset) {
                    addToDocument(document, fields);
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, sort == null, sort == null);
                CompositeAggregationBuilder aggregationBuilder = create.get();
                if (sort != null) {
                    CompositeAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, indexSettings, FIELD_TYPES);
                    assertTrue(aggregator.canEarlyTerminate());
                }
                final InternalComposite composite;
                if (reduced) {
                    composite = searchAndReduce(indexSearcher, query, aggregationBuilder, FIELD_TYPES);
                } else {
                    composite = search(indexSearcher, query, aggregationBuilder, indexSettings, FIELD_TYPES);
                }
                verify.accept(composite);
            }
        }
    }

    private static IndexSettings createIndexSettings(Sort sort) {
        Settings.Builder builder = Settings.builder();
        if (sort != null) {
            String[] fields = Arrays.stream(sort.getSort())
                .map(SortField::getField)
                .toArray(String[]::new);
            String[] orders = Arrays.stream(sort.getSort())
                .map((o) -> o.getReverse() ? "desc" : "asc")
                .toArray(String[]::new);
            builder.putList("index.sort.field", fields);
            builder.putList("index.sort.order", orders);
        }
        return IndexSettingsModule.newIndexSettings(new Index("_index", "0"), builder.build());
    }

    private void addToDocument(Document doc, Map<String, List<Object>> keys) {
        for (Map.Entry<String, List<Object>> entry : keys.entrySet()) {
            final String name = entry.getKey();
            for (Object value : entry.getValue()) {
                if (value instanceof Long) {
                    doc.add(new SortedNumericDocValuesField(name, (long) value));
                } else if (value instanceof Double) {
                    doc.add(new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong((double) value)));
                } else if (value instanceof String) {
                    doc.add(new SortedSetDocValuesField(name, new BytesRef((String) value)));
                } else {
                    throw new AssertionError("invalid object: " + value.getClass().getSimpleName());
                }
            }
        }
    }


    @SuppressWarnings("unchecked")
    private static Map<String, Object> createAfterKey(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fields.length; i+=2) {
            String field = (String) fields[i];
            map.put(field, fields[i+1]);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<Object>> createDocument(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, List<Object>> map = new HashMap<>();
        for (int i = 0; i < fields.length; i+=2) {
            String field = (String) fields[i];
            if (fields[i+1] instanceof List) {
                map.put(field, (List<Object>) fields[i+1]);
            } else {
                map.put(field, Collections.singletonList(fields[i+1]));
            }
        }
        return map;
    }

    private static long asLong(String dateTime) {
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime(dateTime).getMillis();
    }
}
