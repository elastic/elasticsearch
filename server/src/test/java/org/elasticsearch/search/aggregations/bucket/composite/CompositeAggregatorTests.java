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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CompositeAggregatorTests extends AggregatorTestCase {
    private static MappedFieldType[] FIELD_TYPES;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        FIELD_TYPES = new MappedFieldType[8];
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

        FIELD_TYPES[5] = new KeywordFieldMapper.KeywordFieldType();
        FIELD_TYPES[5].setName("terms");
        FIELD_TYPES[5].setHasDocValues(true);

        FIELD_TYPES[6] = new IpFieldMapper.IpFieldType();
        FIELD_TYPES[6].setName("ip");
        FIELD_TYPES[6].setHasDocValues(true);

        FIELD_TYPES[7] = new GeoPointFieldMapper.GeoPointFieldType();
        FIELD_TYPES[7].setName("geo_point");
        FIELD_TYPES[7].setHasDocValues(true);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        FIELD_TYPES = null;
    }

    public void testUnmappedField() throws Exception {
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> new CompositeAggregationBuilder("name",
                Arrays.asList(
                    new TermsValuesSourceBuilder("unmapped").field("unmapped")
                )
            ),
            (result) -> {
                assertEquals(0, result.getBuckets().size());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> new CompositeAggregationBuilder("name",
                Arrays.asList(
                    new TermsValuesSourceBuilder("unmapped").field("unmapped").missingBucket(true)
                )
            ),
            (result) -> {
                assertEquals(1, result.getBuckets().size());
                assertEquals("{unmapped=null}", result.afterKey().toString());
                assertEquals("{unmapped=null}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(5L, result.getBuckets().get(0).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> new CompositeAggregationBuilder("name",
                Arrays.asList(
                    new TermsValuesSourceBuilder("unmapped").field("unmapped").missingBucket(true)
                )).aggregateAfter(Collections.singletonMap("unmapped", null)),
            (result) -> {
                assertEquals(0, result.getBuckets().size());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> new CompositeAggregationBuilder("name",
                Arrays.asList(
                    new TermsValuesSourceBuilder("keyword").field("keyword"),
                    new TermsValuesSourceBuilder("unmapped").field("unmapped")
                )
            ),
            (result) -> {
                assertEquals(0, result.getBuckets().size());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> new CompositeAggregationBuilder("name",
                Arrays.asList(
                    new TermsValuesSourceBuilder("keyword").field("keyword"),
                    new TermsValuesSourceBuilder("unmapped").field("unmapped").missingBucket(true)
                )
            ),
            (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=d, unmapped=null}", result.afterKey().toString());
                assertEquals("{keyword=a, unmapped=null}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=c, unmapped=null}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d, unmapped=null}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=d}", result.afterKey().toString());
                assertEquals("{keyword=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "a"));
            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=d}", result.afterKey().toString());
                assertEquals("{keyword=c}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
            }
        );
    }

    public void testWithKeywordAndMissingBucket() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "a"),
                createDocument("long", 0L),
                createDocument("keyword", "c"),
                createDocument("keyword", "a"),
                createDocument("keyword", "d"),
                createDocument("keyword", "c"),
                createDocument("long", 5L)
            )
        );

        // sort ascending, null bucket is first
        testSearchCase(Arrays.asList(new MatchAllDocsQuery()), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .missingBucket(true);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=d}", result.afterKey().toString());
                assertEquals("{keyword=null}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=a}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=c}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
            }
        );

        // sort descending, null bucket is last
        testSearchCase(Arrays.asList(new MatchAllDocsQuery()), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .missingBucket(true)
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=null}", result.afterKey().toString());
                assertEquals("{keyword=null}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=a}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .missingBucket(true);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", null));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=d}", result.afterKey().toString());
                assertEquals("{keyword=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .missingBucket(true)
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", null));
            }, (result) -> {
                assertEquals(0, result.getBuckets().size());
                assertNull(result.afterKey());
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=zoo}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "car"));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=zoo}", result.afterKey().toString());
                assertEquals("{keyword=delta}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=foo}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=zoo}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword").order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "mar"));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=bar}", result.afterKey().toString());
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=a}", result.afterKey().toString());
                assertEquals("{keyword=a}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=d}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "c"));

            }, (result) -> {
                assertEquals(result.afterKey().toString(), "{keyword=a}");
                assertEquals("{keyword=a}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));

            }, (result) -> {
                    assertEquals(5, result.getBuckets().size());
                    assertEquals("{keyword=z}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "b"));

            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=z}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));

            }, (result) -> {
                assertEquals(5, result.getBuckets().size());
                assertEquals("{keyword=a}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword")
                    .order(SortOrder.DESC);
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "c"));

            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=a}", result.afterKey().toString());
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    )
            ),
            (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=d, long=10}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    )
                ).aggregateAfter(createAfterKey("keyword", "a", "long", 100L)
            ),
            (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=d, long=10}", result.afterKey().toString());
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword").order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("long").field("long").order(SortOrder.DESC)
                    )
                ),
            (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=a, long=0}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword").order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("long").field("long").order(SortOrder.DESC)
                    )).aggregateAfter(createAfterKey("keyword", "d", "long", 10L)
                ), (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=a, long=0}", result.afterKey().toString());
                assertEquals("{keyword=a, long=0}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
            }
        );
    }

    public void testWithKeywordLongAndMissingBucket() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "a", "long", 100L),
                createDocument("double", 0d),
                createDocument("keyword", "c", "long", 100L),
                createDocument("keyword", "a", "long", 0L),
                createDocument("keyword", "d", "long", 10L),
                createDocument("keyword", "c"),
                createDocument("keyword", "c", "long", 100L),
                createDocument("long", 100L),
                createDocument("double", 0d)
            )
        );
        testSearchCase(Arrays.asList(new MatchAllDocsQuery()), dataset,
            () -> new CompositeAggregationBuilder("name",
                Arrays.asList(
                    new TermsValuesSourceBuilder("keyword").field("keyword").missingBucket(true),
                    new TermsValuesSourceBuilder("long").field("long").missingBucket(true)
                )
            ),
            (result) -> {
                assertEquals(7, result.getBuckets().size());
                assertEquals("{keyword=d, long=10}", result.afterKey().toString());
                assertEquals("{keyword=null, long=null}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=null, long=100}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{keyword=a, long=0}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{keyword=c, long=null}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(4).getDocCount());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(5).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(5).getDocCount());
                assertEquals("{keyword=d, long=10}", result.getBuckets().get(6).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(6).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> new CompositeAggregationBuilder("name",
                Arrays.asList(
                    new TermsValuesSourceBuilder("keyword").field("keyword").missingBucket(true),
                    new TermsValuesSourceBuilder("long").field("long").missingBucket(true)
                )
            ).aggregateAfter(createAfterKey("keyword", "c", "long", null)
            ),
            (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=d, long=10}", result.afterKey().toString());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=d, long=10}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    ))
            , (result) -> {
                assertEquals(10, result.getBuckets().size());
                assertEquals("{keyword=z, long=0}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    )
                ).aggregateAfter(createAfterKey("keyword", "c", "long", 10L))
            , (result) -> {
                assertEquals(6, result.getBuckets().size());
                assertEquals("{keyword=z, long=100}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
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
                assertEquals("{keyword=a, long=0}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
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
                assertEquals("{keyword=a, long=0}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
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
                assertEquals("{keyword=c, long=100, double=0.4}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
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
                assertEquals("{keyword=z, long=0, double=0.09}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long"),
                        new TermsValuesSourceBuilder("double").field("double")
                    )
                ).aggregateAfter(createAfterKey("keyword", "z", "long", 100L, "double", 0.4d))
            , (result) -> {
                assertEquals(0, result.getBuckets().size());
                assertNull(result.afterKey());
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date"),
            LongPoint.newRangeQuery(
                "date",
                asLong("2016-09-20T09:00:34"),
                asLong("2017-10-20T06:09:24")
            )), dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1));
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo));
            },
            (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{date=1508457600000}", result.afterKey().toString());
                assertEquals("{date=1474329600000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508371200000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=1508457600000}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date"),
            LongPoint.newRangeQuery(
                "date",
                asLong("2016-09-20T11:34:00"),
                asLong("2017-10-20T06:09:24")
            )), dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1));
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo))
                    .aggregateAfter(createAfterKey("date", 1474329600000L));

            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{date=1508457600000}", result.afterKey().toString());
                assertEquals("{date=1508371200000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508457600000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
            }
        );

        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testWithDateTerms() throws IOException {
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date"),
            LongPoint.newRangeQuery(
                "date",
                asLong("2016-09-20T09:00:34"),
                asLong("2017-10-20T06:09:24")
            )), dataset,
            () -> {
                TermsValuesSourceBuilder histo = new TermsValuesSourceBuilder("date")
                    .field("date");
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo));
            },
            (result) -> {
                assertEquals(5, result.getBuckets().size());
                assertEquals("{date=1508479764000}", result.afterKey().toString());
                assertThat(result.getBuckets().get(0).getKey().get("date"), instanceOf(Long.class));
                assertEquals("{date=1474362034000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1474371240000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=1508393364000}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                assertEquals("{date=1508468925000}", result.getBuckets().get(3).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(3).getDocCount());
                assertEquals("{date=1508479764000}", result.getBuckets().get(4).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(4).getDocCount());
            }
        );
    }

    public void testWithDateHistogramAndFormat() throws IOException {
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date")), dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1))
                    .format("yyyy-MM-dd");
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo));
            },
            (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{date=2017-10-20}", result.afterKey().toString());
                assertEquals("{date=2016-09-20}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=2017-10-19}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=2017-10-20}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date")), dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1))
                    .format("yyyy-MM-dd");
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo))
                    .aggregateAfter(createAfterKey("date", "2016-09-20"));

            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{date=2017-10-20}", result.afterKey().toString());
                assertEquals("{date=2017-10-19}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=2017-10-20}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
            }
        );

        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testThatDateHistogramFailsFormatAfter() throws IOException {
        ElasticsearchParseException exc = expectThrows(ElasticsearchParseException.class,
            () -> testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date")), Collections.emptyList(),
                () -> {
                    DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                        .field("date")
                        .dateHistogramInterval(DateHistogramInterval.days(1))
                        .format("yyyy-MM-dd");
                    return new CompositeAggregationBuilder("name", Collections.singletonList(histo))
                        .aggregateAfter(createAfterKey("date", "now"));
                },
                (result) -> {}
        ));
        assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exc.getCause().getMessage(), containsString("now() is not supported in [after] key"));

        exc = expectThrows(ElasticsearchParseException.class,
            () -> testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date")), Collections.emptyList(),
                () -> {
                    DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                        .field("date")
                        .dateHistogramInterval(DateHistogramInterval.days(1))
                        .format("yyyy-MM-dd");
                    return new CompositeAggregationBuilder("name", Collections.singletonList(histo))
                        .aggregateAfter(createAfterKey("date", "1474329600000"));
                },
                (result) -> {}
            ));
        assertThat(exc.getMessage(), containsString("failed to parse date field [1474329600000]"));
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date")), dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1))
                    .timeZone(ZoneOffset.ofHours(1));
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo));
            },
            (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{date=1508454000000}", result.afterKey().toString());
                assertEquals("{date=1474326000000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508367600000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=1508454000000}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date")), dataset,
            () -> {
                DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder("date")
                    .field("date")
                    .dateHistogramInterval(DateHistogramInterval.days(1))
                    .timeZone(ZoneOffset.ofHours(1));
                return new CompositeAggregationBuilder("name", Collections.singletonList(histo))
                    .aggregateAfter(createAfterKey("date", 1474326000000L));

            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{date=1508454000000}", result.afterKey().toString());
                assertEquals("{date=1508367600000}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508454000000}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
            }
        );

        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date"),
            LongPoint.newRangeQuery(
                "date",
                asLong("2016-09-20T09:00:34"),
                asLong("2017-10-20T06:09:24")
            )), dataset,
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
                assertEquals("{date=1508457600000, keyword=d}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("date"),
            LongPoint.newRangeQuery(
                "date",
                asLong("2016-09-20T11:34:00"),
                asLong("2017-10-20T06:09:24")
            )), dataset,
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
                assertEquals("{date=1508457600000, keyword=d}", result.afterKey().toString());
                assertEquals("{date=1508457600000, keyword=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{date=1508457600000, keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{date=1508457600000, keyword=d}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        );

        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("price")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new HistogramValuesSourceBuilder("price").field("price").interval(10)
                    )
                )
            , (result) -> {
                assertEquals(7, result.getBuckets().size());
                assertEquals("{keyword=z, price=50.0}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("price")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new HistogramValuesSourceBuilder("price").field("price").interval(10)
                    )
                ).aggregateAfter(createAfterKey("keyword", "c", "price", 50.0))
            , (result) -> {
                assertEquals(4, result.getBuckets().size());
                assertEquals("{keyword=z, price=50.0}", result.afterKey().toString());
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("double")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new HistogramValuesSourceBuilder("histo").field("double").interval(0.1),
                        new TermsValuesSourceBuilder("keyword").field("keyword")
                    )
                )
            , (result) -> {
                assertEquals(8, result.getBuckets().size());
                assertEquals("{histo=0.9, keyword=d}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("double")), dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new HistogramValuesSourceBuilder("histo").field("double").interval(0.1),
                        new TermsValuesSourceBuilder("keyword").field("keyword")
                    )
                ).aggregateAfter(createAfterKey("histo", 0.8d, "keyword", "b"))
            , (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{histo=0.9, keyword=d}", result.afterKey().toString());
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
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
                assertEquals("{keyword=z, date_histo=1474329600000}", result.afterKey().toString());
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

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
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
                assertEquals("{keyword=z, date_histo=1474329600000}", result.afterKey().toString());
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

        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testWithKeywordAndTopHits() throws Exception {
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
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .subAggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{keyword=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                TopHits topHits = result.getBuckets().get(0).getAggregations().get("top_hits");
                assertNotNull(topHits);
                assertEquals(topHits.getHits().getHits().length, 2);
                assertEquals(topHits.getHits().getTotalHits().value, 2L);
                assertEquals("{keyword=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                topHits = result.getBuckets().get(1).getAggregations().get("top_hits");
                assertNotNull(topHits);
                assertEquals(topHits.getHits().getHits().length, 2);
                assertEquals(topHits.getHits().getTotalHits().value, 2L);
                assertEquals("{keyword=d}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
                topHits = result.getBuckets().get(2).getAggregations().get("top_hits");
                assertNotNull(topHits);
                assertEquals(topHits.getHits().getHits().length, 1);
                assertEquals(topHits.getHits().getTotalHits().value, 1L);
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                    .field("keyword");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("keyword", "a"))
                    .subAggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"));
            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=c}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                TopHits topHits = result.getBuckets().get(0).getAggregations().get("top_hits");
                assertNotNull(topHits);
                assertEquals(topHits.getHits().getHits().length, 2);
                assertEquals(topHits.getHits().getTotalHits().value, 2L);
                assertEquals("{keyword=d}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                topHits = result.getBuckets().get(1).getAggregations().get("top_hits");
                assertNotNull(topHits);
                assertEquals(topHits.getHits().getHits().length, 1);
                assertEquals(topHits.getHits().getTotalHits().value, 1L);
            }
        );
    }

    public void testWithTermsSubAggExecutionMode() throws Exception {
        // test with no bucket
        for (Aggregator.SubAggCollectionMode mode : Aggregator.SubAggCollectionMode.values()) {
            testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")),
                Collections.singletonList(createDocument()),
                () -> {
                    TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                        .field("keyword");
                    return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                        .subAggregation(
                            new TermsAggregationBuilder("terms", ValueType.STRING)
                                .field("terms")
                                .collectMode(mode)
                                .subAggregation(new MaxAggregationBuilder("max").field("long"))
                        );
                }, (result) -> {
                    assertEquals(0, result.getBuckets().size());
                }
            );
        }

        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "a", "terms", "a", "long", 50L),
                createDocument("keyword", "c", "terms", "d", "long", 78L),
                createDocument("keyword", "a", "terms", "w", "long", 78L),
                createDocument("keyword", "d", "terms", "y", "long", 76L),
                createDocument("keyword", "c", "terms", "y", "long", 70L)
            )
        );
        for (Aggregator.SubAggCollectionMode mode : Aggregator.SubAggCollectionMode.values()) {
            testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("keyword")), dataset,
                () -> {
                    TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("keyword")
                        .field("keyword");
                    return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                        .subAggregation(
                            new TermsAggregationBuilder("terms", ValueType.STRING)
                                .field("terms")
                                .collectMode(mode)
                                .subAggregation(new MaxAggregationBuilder("max").field("long"))
                        );
                }, (result) -> {
                    assertEquals(3, result.getBuckets().size());

                    assertEquals("{keyword=a}", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(2L, result.getBuckets().get(0).getDocCount());
                    StringTerms subTerms = result.getBuckets().get(0).getAggregations().get("terms");
                    assertEquals(2, subTerms.getBuckets().size());
                    assertEquals("a", subTerms.getBuckets().get(0).getKeyAsString());
                    assertEquals("w", subTerms.getBuckets().get(1).getKeyAsString());
                    InternalMax max = subTerms.getBuckets().get(0).getAggregations().get("max");
                    assertEquals(50L, (long) max.getValue());
                    max = subTerms.getBuckets().get(1).getAggregations().get("max");
                    assertEquals(78L, (long) max.getValue());

                    assertEquals("{keyword=c}", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(2L, result.getBuckets().get(1).getDocCount());
                    subTerms = result.getBuckets().get(1).getAggregations().get("terms");
                    assertEquals(2, subTerms.getBuckets().size());
                    assertEquals("d", subTerms.getBuckets().get(0).getKeyAsString());
                    assertEquals("y", subTerms.getBuckets().get(1).getKeyAsString());
                    max = subTerms.getBuckets().get(0).getAggregations().get("max");
                    assertEquals(78L, (long) max.getValue());
                    max = subTerms.getBuckets().get(1).getAggregations().get("max");
                    assertEquals(70L, (long) max.getValue());

                    assertEquals("{keyword=d}", result.getBuckets().get(2).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    subTerms = result.getBuckets().get(2).getAggregations().get("terms");
                    assertEquals(1, subTerms.getBuckets().size());
                    assertEquals("y", subTerms.getBuckets().get(0).getKeyAsString());
                    max = subTerms.getBuckets().get(0).getAggregations().get("max");
                    assertEquals(76L, (long) max.getValue());
                }
            );
        }
    }

    public void testRandomStrings() throws IOException {
        testRandomTerms("keyword", () -> randomAlphaOfLengthBetween(5, 50), (v) -> (String) v);
    }

    public void testRandomLongs() throws IOException {
        testRandomTerms("long", () -> randomLong(), (v) -> (long) v);
    }

    public void testRandomInts() throws IOException {
        testRandomTerms("price", () -> randomInt(), (v) -> ((Number) v).intValue());
    }

    public void testDuplicateNames() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
            builders.add(new TermsValuesSourceBuilder("duplicate1").field("bar"));
            builders.add(new TermsValuesSourceBuilder("duplicate1").field("baz"));
            builders.add(new TermsValuesSourceBuilder("duplicate2").field("bar"));
            builders.add(new TermsValuesSourceBuilder("duplicate2").field("baz"));
           new CompositeAggregationBuilder("foo", builders);
        });
        assertThat(e.getMessage(), equalTo("Composite source names must be unique, found duplicates: [duplicate2, duplicate1]"));
    }

    public void testMissingSources() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
            new CompositeAggregationBuilder("foo", builders);
        });
        assertThat(e.getMessage(), equalTo("Composite [sources] cannot be null or empty"));

        e = expectThrows(IllegalArgumentException.class, () -> new CompositeAggregationBuilder("foo", null));
        assertThat(e.getMessage(), equalTo("Composite [sources] cannot be null or empty"));
    }

    public void testNullSourceNonNullCollection() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
            builders.add(null);
            new CompositeAggregationBuilder("foo", builders);
        });
        assertThat(e.getMessage(), equalTo("Composite source cannot be null"));
    }

    private <T extends Comparable<T>, V extends Comparable<T>> void testRandomTerms(String field,
                                                                                    Supplier<T> randomSupplier,
                                                                                    Function<Object, V> transformKey) throws IOException {
        int numTerms = randomIntBetween(10, 500);
        List<T> terms = new ArrayList<>();
        for (int i = 0; i < numTerms; i++) {
            terms.add(randomSupplier.get());
        }
        int numDocs = randomIntBetween(100, 200);
        List<Map<String, List<Object>>> dataset = new ArrayList<>();

        Set<T> valuesSet = new HashSet<>();
        Map<Comparable<?>, AtomicLong> expectedDocCounts = new HashMap<> ();
        for (int i = 0; i < numDocs; i++) {
            int numValues = randomIntBetween(1, 5);
            Set<Object> values = new HashSet<>();
            for (int j = 0; j < numValues; j++) {
                int rand = randomIntBetween(0, terms.size() - 1);
                if (values.add(terms.get(rand))) {
                    AtomicLong count = expectedDocCounts.computeIfAbsent(terms.get(rand),
                        (k) -> new AtomicLong(0));
                    count.incrementAndGet();
                    valuesSet.add(terms.get(rand));
                }
            }
            dataset.add(Collections.singletonMap(field, new ArrayList<>(values)));
        }
        List<T> expected = new ArrayList<>(valuesSet);
        Collections.sort(expected);

        List<Comparable<T>> seen = new ArrayList<>();
        AtomicBoolean finish = new AtomicBoolean(false);
        int size = randomIntBetween(1,  expected.size());
        while (finish.get() == false) {
            testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery(field)), dataset,
                () -> {
                    Map<String, Object> afterKey = null;
                    if (seen.size() > 0) {
                        afterKey = Collections.singletonMap(field, seen.get(seen.size()-1));
                    }
                    TermsValuesSourceBuilder source = new TermsValuesSourceBuilder(field).field(field);
                    return new CompositeAggregationBuilder("name", Collections.singletonList(source))
                        .subAggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"))
                        .aggregateAfter(afterKey)
                        .size(size);
                }, (result) -> {
                    if (result.getBuckets().size() == 0) {
                        finish.set(true);
                    }
                    for (InternalComposite.InternalBucket bucket : result.getBuckets()) {
                        V term = transformKey.apply(bucket.getKey().get(field));
                        seen.add(term);
                        assertThat(bucket.getDocCount(), equalTo(expectedDocCounts.get(term).get()));
                    }
                });
        }
        assertEquals(expected, seen);
    }

    public void testWithIP() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("ip", InetAddress.getByName("127.0.0.1")),
                createDocument("ip", InetAddress.getByName("192.168.0.1")),
                createDocument("ip", InetAddress.getByName("::1")),
                createDocument("ip", InetAddress.getByName("::1")),
                createDocument("ip", InetAddress.getByName("192.168.0.1"))
            )
        );
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("ip")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("ip")
                    .field("ip");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms));
            }, (result) -> {
                assertEquals(3, result.getBuckets().size());
                assertEquals("{ip=192.168.0.1}", result.afterKey().toString());
                assertEquals("{ip=::1}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{ip=127.0.0.1}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertEquals("{ip=192.168.0.1}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("ip")), dataset,
            () -> {
                TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("ip")
                    .field("ip");
                return new CompositeAggregationBuilder("name", Collections.singletonList(terms))
                    .aggregateAfter(Collections.singletonMap("ip", "::1"));
            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{ip=192.168.0.1}", result.afterKey().toString());
                assertEquals("{ip=127.0.0.1}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{ip=192.168.0.1}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
            }
        );
    }

    public void testWithGeoPoint() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("geo_point", new GeoPoint(48.934059, 41.610741)),
                createDocument("geo_point", new GeoPoint(-23.065941, 113.610741)),
                createDocument("geo_point", new GeoPoint(90.0, 0.0)),
                createDocument("geo_point", new GeoPoint(37.2343, -115.8067)),
                createDocument("geo_point", new GeoPoint(90.0, 0.0))
            )
        );
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("geo_point")), dataset,
            () -> {
                GeoTileGridValuesSourceBuilder geoTile = new GeoTileGridValuesSourceBuilder("geo_point")
                    .field("geo_point");
                return new CompositeAggregationBuilder("name", Collections.singletonList(geoTile));
            }, (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{geo_point=7/64/56}", result.afterKey().toString());
                assertEquals("{geo_point=7/32/56}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{geo_point=7/64/56}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(3L, result.getBuckets().get(1).getDocCount());
            }
        );

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("geo_point")), dataset,
            () -> {
                GeoTileGridValuesSourceBuilder geoTile = new GeoTileGridValuesSourceBuilder("geo_point")
                    .field("geo_point");
                return new CompositeAggregationBuilder("name", Collections.singletonList(geoTile))
                    .aggregateAfter(Collections.singletonMap("geo_point", "7/32/56"));
            }, (result) -> {
                assertEquals(1, result.getBuckets().size());
                assertEquals("{geo_point=7/64/56}", result.afterKey().toString());
                assertEquals("{geo_point=7/64/56}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(3L, result.getBuckets().get(0).getDocCount());
            }
        );
    }

    private void testSearchCase(List<Query> queries,
                                List<Map<String, List<Object>>> dataset,
                                Supplier<CompositeAggregationBuilder> create,
                                Consumer<InternalComposite> verify) throws IOException {
        for (Query query : queries) {
            executeTestCase(false, query, dataset, create, verify);
            executeTestCase(true, query, dataset, create, verify);
        }
    }

    private void executeTestCase(boolean reduced,
                                 Query query,
                                 List<Map<String, List<Object>>> dataset,
                                 Supplier<CompositeAggregationBuilder> create,
                                 Consumer<InternalComposite> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (Map<String, List<Object>> fields : dataset) {
                    addToDocument(document, fields);
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                CompositeAggregationBuilder aggregationBuilder = create.get();
                final InternalComposite composite;
                if (reduced) {
                    composite = searchAndReduce(indexSearcher, query, aggregationBuilder, FIELD_TYPES);
                } else {
                    composite = search(indexSearcher, query, aggregationBuilder, FIELD_TYPES);
                }
                verify.accept(composite);
            }
        }
    }

    private void addToDocument(Document doc, Map<String, List<Object>> keys) {
        for (Map.Entry<String, List<Object>> entry : keys.entrySet()) {
            final String name = entry.getKey();
            for (Object value : entry.getValue()) {
                if (value instanceof Integer) {
                    doc.add(new SortedNumericDocValuesField(name, (int) value));
                    doc.add(new IntPoint(name, (int) value));
                } else if (value instanceof Long) {
                    doc.add(new SortedNumericDocValuesField(name, (long) value));
                    doc.add(new LongPoint(name, (long) value));
                } else if (value instanceof Double) {
                    doc.add(new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong((double) value)));
                    doc.add(new DoublePoint(name, (double) value));
                } else if (value instanceof String) {
                    doc.add(new SortedSetDocValuesField(name, new BytesRef((String) value)));
                    doc.add(new StringField(name, new BytesRef((String) value), Field.Store.NO));
                } else if (value instanceof InetAddress) {
                    doc.add(new SortedSetDocValuesField(name, new BytesRef(InetAddressPoint.encode((InetAddress) value))));
                    doc.add(new InetAddressPoint(name, (InetAddress) value));
                } else if (value instanceof GeoPoint) {
                    GeoPoint point = (GeoPoint)value;
                    doc.add(new SortedNumericDocValuesField(name,
                        GeoTileUtils.longEncode(point.lon(), point.lat(), GeoTileGridAggregationBuilder.DEFAULT_PRECISION)));
                    doc.add(new LatLonPoint(name, point.lat(), point.lon()));
                } else {
                    throw new AssertionError("invalid object: " + value.getClass().getSimpleName());
                }
            }
        }
    }

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
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }
}
