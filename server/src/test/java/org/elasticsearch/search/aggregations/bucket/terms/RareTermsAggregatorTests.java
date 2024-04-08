/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregatorTests;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME;
import static org.hamcrest.Matchers.equalTo;

public class RareTermsAggregatorTests extends AggregatorTestCase {

    private static final String LONG_FIELD = "numeric";
    private static final String KEYWORD_FIELD = "keyword";

    private static final List<Long> dataset;
    static {
        List<Long> d = new ArrayList<>(45);
        for (long i = 0; i < 10; i++) {
            for (int j = 0; j < i; j++) {
                d.add(i);
            }
        }
        dataset = d;
    }

    public void testMatchNoDocs() throws IOException {
        testSearchCase(
            new MatchNoDocsQuery(),
            dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size())
        );
        testSearchCase(
            new MatchNoDocsQuery(),
            dataset,
            aggregation -> aggregation.field(LONG_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size())
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset, aggregation -> aggregation.field(LONG_FIELD).maxDocCount(1), agg -> {
            assertEquals(1, agg.getBuckets().size());
            LongRareTerms.Bucket bucket = (LongRareTerms.Bucket) agg.getBuckets().get(0);
            assertThat(bucket.getKey(), equalTo(1L));
            assertThat(bucket.getDocCount(), equalTo(1L));
        });
        testSearchCase(query, dataset, aggregation -> aggregation.field(KEYWORD_FIELD).maxDocCount(1), agg -> {
            assertEquals(1, agg.getBuckets().size());
            StringRareTerms.Bucket bucket = (StringRareTerms.Bucket) agg.getBuckets().get(0);
            assertThat(bucket.getKeyAsString(), equalTo("1"));
            assertThat(bucket.getDocCount(), equalTo(1L));
        });
    }

    public void testManyDocsOneRare() throws IOException {
        Query query = new MatchAllDocsQuery();

        List<Long> d = new ArrayList<>(500);
        for (int i = 1; i < 500; i++) {
            d.add((long) i);
            d.add((long) i);
        }

        // The one rare term
        d.add(0L);

        testSearchCase(query, d, aggregation -> aggregation.field(LONG_FIELD).maxDocCount(1), agg -> {
            assertEquals(1, agg.getBuckets().size());
            LongRareTerms.Bucket bucket = (LongRareTerms.Bucket) agg.getBuckets().get(0);
            assertThat(bucket.getKey(), equalTo(0L));
            assertThat(bucket.getDocCount(), equalTo(1L));
        });
        testSearchCase(query, d, aggregation -> aggregation.field(KEYWORD_FIELD).maxDocCount(1), agg -> {
            assertEquals(1, agg.getBuckets().size());
            StringRareTerms.Bucket bucket = (StringRareTerms.Bucket) agg.getBuckets().get(0);
            assertThat(bucket.getKeyAsString(), equalTo("0"));
            assertThat(bucket.getDocCount(), equalTo(1L));
        });
    }

    public void testIncludeExclude() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(
            query,
            dataset,
            aggregation -> aggregation.field(LONG_FIELD)
                .maxDocCount(2) // bump to 2 since we're only including "2"
                .includeExclude(new IncludeExclude(null, null, new TreeSet<>(Set.of(new BytesRef("2"))), new TreeSet<>())),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                LongRareTerms.Bucket bucket = (LongRareTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKey(), equalTo(2L));
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        );
    }

    public void testEmbeddedMaxAgg() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset, aggregation -> {
            MaxAggregationBuilder max = new MaxAggregationBuilder("the_max").field(LONG_FIELD);
            aggregation.field(LONG_FIELD).maxDocCount(1).subAggregation(max);
        }, agg -> {
            assertEquals(1, agg.getBuckets().size());
            LongRareTerms.Bucket bucket = (LongRareTerms.Bucket) agg.getBuckets().get(0);
            assertThat(bucket.getKey(), equalTo(1L));
            assertThat(bucket.getDocCount(), equalTo(1L));

            InternalAggregations children = bucket.getAggregations();
            assertThat(children.asList().size(), equalTo(1));
            assertThat(children.asList().get(0).getName(), equalTo("the_max"));
            assertThat(((Max) (children.asList().get(0))).value(), equalTo(1.0));
        });
        testSearchCase(query, dataset, aggregation -> {
            MaxAggregationBuilder max = new MaxAggregationBuilder("the_max").field(LONG_FIELD);
            aggregation.field(KEYWORD_FIELD).maxDocCount(1).subAggregation(max);
        }, agg -> {
            assertEquals(1, agg.getBuckets().size());
            StringRareTerms.Bucket bucket = (StringRareTerms.Bucket) agg.getBuckets().get(0);
            assertThat(bucket.getKey(), equalTo("1"));
            assertThat(bucket.getDocCount(), equalTo(1L));

            InternalAggregations children = bucket.getAggregations();
            assertThat(children.asList().size(), equalTo(1));
            assertThat(children.asList().get(0).getName(), equalTo("the_max"));
            assertThat(((Max) (children.asList().get(0))).value(), equalTo(1.0));
        });
    }

    public void testEmpty() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(
            query,
            Collections.emptyList(),
            aggregation -> aggregation.field(LONG_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size())
        );
        testSearchCase(
            query,
            Collections.emptyList(),
            aggregation -> aggregation.field(KEYWORD_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size())
        );

        testSearchCase(
            query,
            Collections.emptyList(),
            aggregation -> aggregation.field(LONG_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size())
        );
        testSearchCase(
            query,
            Collections.emptyList(),
            aggregation -> aggregation.field(KEYWORD_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size())
        );
    }

    public void testUnmapped() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedDocValuesField("string", new BytesRef("a")));
                document.add(new NumericDocValuesField("long", 0L));
                indexWriter.addDocument(document);
                MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType("another_string");
                MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("another_long", NumberFieldMapper.NumberType.LONG);

                try (DirectoryReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    String[] fieldNames = new String[] { "string", "long" };
                    for (int i = 0; i < fieldNames.length; i++) {
                        RareTermsAggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name").field(fieldNames[i]);
                        RareTerms result = searchAndReduce(indexReader, new AggTestConfig(aggregationBuilder, fieldType1, fieldType2));
                        assertEquals("_name", result.getName());
                        assertEquals(0, result.getBuckets().size());
                    }
                }
            }
        }
    }

    public void testRangeField() throws Exception {
        RangeType rangeType = RangeType.DOUBLE;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                    new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true), // bucket 0 5
                    new RangeFieldMapper.Range(rangeType, -3.1, 4.2, true, true), // bucket -5, 0
                    new RangeFieldMapper.Range(rangeType, 4.2, 13.3, true, true), // bucket 0, 5, 10
                    new RangeFieldMapper.Range(rangeType, 42.5, 49.3, true, true), // bucket 40, 45
                }) {
                    Document doc = new Document();
                    BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                    doc.add(new BinaryDocValuesField("field", encodedRange));
                    indexWriter.addDocument(doc);
                }
                MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType("field", rangeType);

                try (DirectoryReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    RareTermsAggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name").field("field");
                    expectThrows(
                        IllegalArgumentException.class,
                        () -> searchAndReduce(indexReader, new AggTestConfig(aggregationBuilder, fieldType))
                    );
                }
            }
        }
    }

    public void testNestedTerms() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset, aggregation -> {
            TermsAggregationBuilder terms = new TermsAggregationBuilder("the_terms").field(KEYWORD_FIELD);
            aggregation.field(LONG_FIELD).maxDocCount(1).subAggregation(terms);
        }, agg -> {
            assertEquals(1, agg.getBuckets().size());
            LongRareTerms.Bucket bucket = (LongRareTerms.Bucket) agg.getBuckets().get(0);
            assertThat(bucket.getKey(), equalTo(1L));
            assertThat(bucket.getDocCount(), equalTo(1L));

            InternalAggregations children = bucket.getAggregations();
            assertThat(children.asList().size(), equalTo(1));
            assertThat(children.asList().get(0).getName(), equalTo("the_terms"));
            assertThat(((Terms) (children.asList().get(0))).getBuckets().size(), equalTo(1));
            assertThat(((Terms) (children.asList().get(0))).getBuckets().get(0).getKeyAsString(), equalTo("1"));
        });

        testSearchCase(query, dataset, aggregation -> {
            TermsAggregationBuilder terms = new TermsAggregationBuilder("the_terms").field(KEYWORD_FIELD);
            aggregation.field(KEYWORD_FIELD).maxDocCount(1).subAggregation(terms);
        }, agg -> {
            assertEquals(1, agg.getBuckets().size());
            StringRareTerms.Bucket bucket = (StringRareTerms.Bucket) agg.getBuckets().get(0);
            assertThat(bucket.getKey(), equalTo("1"));
            assertThat(bucket.getDocCount(), equalTo(1L));

            InternalAggregations children = bucket.getAggregations();
            assertThat(children.asList().size(), equalTo(1));
            assertThat(children.asList().get(0).getName(), equalTo("the_terms"));
            assertThat(((Terms) (children.asList().get(0))).getBuckets().size(), equalTo(1));
            assertThat(((Terms) (children.asList().get(0))).getBuckets().get(0).getKeyAsString(), equalTo("1"));
        });
    }

    public void testInsideTerms() throws IOException {
        for (String field : new String[] { KEYWORD_FIELD, LONG_FIELD }) {
            AggregationBuilder builder = new TermsAggregationBuilder("terms").field("even_odd")
                .subAggregation(new RareTermsAggregationBuilder("rare").field(field).maxDocCount(2));
            StringTerms terms = executeTestCase(new MatchAllDocsQuery(), dataset, builder);

            StringTerms.Bucket even = terms.getBucketByKey("even");
            InternalRareTerms<?, ?> evenRare = even.getAggregations().get("rare");
            assertEquals(evenRare.getBuckets().stream().map(InternalRareTerms.Bucket::getKeyAsString).collect(toList()), List.of("2"));
            assertEquals(evenRare.getBuckets().stream().map(InternalRareTerms.Bucket::getDocCount).collect(toList()), List.of(2L));

            StringTerms.Bucket odd = terms.getBucketByKey("odd");
            InternalRareTerms<?, ?> oddRare = odd.getAggregations().get("rare");
            assertEquals(oddRare.getBuckets().stream().map(InternalRareTerms.Bucket::getKeyAsString).collect(toList()), List.of("1"));
            assertEquals(oddRare.getBuckets().stream().map(InternalRareTerms.Bucket::getDocCount).collect(toList()), List.of(1L));
        }
    }

    public void testWithNestedAggregations() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 10; i++) {
                    int[] nestedValues = new int[i];
                    for (int j = 0; j < i; j++) {
                        nestedValues[j] = j;
                    }
                    indexWriter.addDocuments(generateDocsWithNested(Integer.toString(i), i, nestedValues));
                }
                indexWriter.commit();

                NestedAggregationBuilder nested = new NestedAggregationBuilder("nested", "nested_object").subAggregation(
                    new RareTermsAggregationBuilder("terms").field("nested_value").maxDocCount(1)
                );
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("nested_value", NumberFieldMapper.NumberType.LONG);
                try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                    AggTestConfig aggTestConfig = new AggTestConfig(nested, fieldType).withQuery(new FieldExistsQuery(PRIMARY_TERM_NAME));
                    // match root document only
                    InternalNested result = searchAndReduce(indexReader, aggTestConfig);
                    InternalMultiBucketAggregation<?, ?> terms = result.getAggregations().get("terms");
                    assertThat(terms.getBuckets().size(), equalTo(1));
                    assertThat(terms.getBuckets().get(0).getKeyAsString(), equalTo("8"));
                }

            }
        }
    }

    public void testWithNestedScoringAggregations() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 10; i++) {
                    int[] nestedValues = new int[i];
                    for (int j = 0; j < i; j++) {
                        nestedValues[j] = j;
                    }
                    indexWriter.addDocuments(generateDocsWithNested(Integer.toString(i), i, nestedValues));
                }
                indexWriter.commit();
                for (boolean withScore : new boolean[] { true, false }) {
                    NestedAggregationBuilder nested = new NestedAggregationBuilder("nested", "nested_object").subAggregation(
                        new RareTermsAggregationBuilder("terms").field("nested_value")
                            .maxDocCount(2)
                            .subAggregation(
                                new TopHitsAggregationBuilder("top_hits").sort(
                                    withScore ? new ScoreSortBuilder() : new FieldSortBuilder("_doc")
                                ).storedField("_none_")
                            )
                    );
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("nested_value", NumberFieldMapper.NumberType.LONG);
                    try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {

                        if (withScore) {

                            IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
                                // match root document only
                                AggTestConfig aggTestConfig = new AggTestConfig(nested, fieldType).withQuery(
                                    new FieldExistsQuery(PRIMARY_TERM_NAME)
                                );
                                searchAndReduce(indexReader, aggTestConfig);
                            });
                            assertThat(
                                e.getMessage(),
                                equalTo(
                                    "RareTerms agg [terms] is the child of the nested agg [nested], and also has a scoring "
                                        + "child agg [top_hits].  This combination is not supported because it requires "
                                        + "executing in [depth_first] mode, which the RareTerms agg cannot do."
                                )
                            );
                        } else {
                            AggTestConfig aggTestConfig = new AggTestConfig(nested, fieldType).withQuery(
                                new FieldExistsQuery(PRIMARY_TERM_NAME)
                            );
                            // match root document only
                            InternalNested result = searchAndReduce(indexReader, aggTestConfig);
                            InternalMultiBucketAggregation<?, ?> terms = result.getAggregations().get("terms");
                            assertThat(terms.getBuckets().size(), equalTo(2));
                            long counter = 1;
                            for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                                InternalTopHits topHits = bucket.getAggregations().get("top_hits");
                                TotalHits hits = topHits.getHits().getTotalHits();
                                assertNotNull(hits);
                                assertThat(hits.value, equalTo(counter));
                                assertThat(topHits.getHits().getMaxScore(), equalTo(Float.NaN));
                                counter += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    private final SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();

    private List<Iterable<IndexableField>> generateDocsWithNested(String id, int value, int[] nestedValues) {
        List<Iterable<IndexableField>> documents = new ArrayList<>();

        for (int nestedValue : nestedValues) {
            Document document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "nested_object", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("nested_value", nestedValue));
            documents.add(document);
        }

        LuceneDocument document = new LuceneDocument();
        document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.YES));
        document.add(new StringField(NestedPathFieldMapper.NAME, "docs", Field.Store.NO));
        document.add(new SortedNumericDocValuesField("value", value));
        sequenceIDFields.addFields(document);
        documents.add(document);

        return documents;
    }

    @Override
    protected IndexSettings createIndexSettings() {
        Settings nodeSettings = Settings.builder().put("search.max_buckets", 100000).build();
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            nodeSettings
        );
    }

    private void testSearchCase(
        Query query,
        List<Long> dataset,
        Consumer<RareTermsAggregationBuilder> configure,
        Consumer<InternalMappedRareTerms<?, ?>> verify
    ) throws IOException {
        RareTermsAggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name");
        if (configure != null) {
            configure.accept(aggregationBuilder);
        }
        verify.accept(executeTestCase(query, dataset, aggregationBuilder));

    }

    private <A extends InternalAggregation> A executeTestCase(Query query, List<Long> dataset, AggregationBuilder aggregationBuilder)
        throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                List<Long> shuffledDataset = new ArrayList<>(dataset);
                Collections.shuffle(shuffledDataset, random());
                for (Long value : shuffledDataset) {
                    document.add(new SortedNumericDocValuesField(LONG_FIELD, value));
                    document.add(new LongPoint(LONG_FIELD, value));
                    document.add(new Field(KEYWORD_FIELD, new BytesRef(Long.toString(value)), KeywordFieldMapper.Defaults.FIELD_TYPE));
                    document.add(
                        new Field("even_odd", new BytesRef(value % 2 == 0 ? "even" : "odd"), KeywordFieldMapper.Defaults.FIELD_TYPE)
                    );
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                MappedFieldType[] types = new MappedFieldType[] {
                    keywordField(KEYWORD_FIELD),
                    longField(LONG_FIELD),
                    keywordField("even_odd") };
                return searchAndReduce(indexReader, new AggTestConfig(aggregationBuilder, types).withQuery(query));
            }
        }
    }

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         *
         * This is used in the aggregator tests to check that after a reduction, we have the correct number of buckets.
         * This can be done during incremental reduces, and the final reduce.  Unfortunately, the number of buckets
         * can _decrease_ during runtime as values are reduced together (e.g. 1 count on each shard, but when
         * reduced it becomes 2 and is greater than the threshold).
         *
         * Because the incremental reduction test picks random subsets to reduce together, it's impossible
         * to predict how the buckets will end up, and so this assertion will fail.
         *
         * If we want to put this assertion back in, we'll need this test to override the incremental reduce
         * portion so that we can deterministically know which shards are being reduced together and which
         * buckets we should have left after each reduction.
         */
    }

    @Override
    protected List<ObjectMapper> objectMappers() {
        return List.of(NestedAggregatorTests.nestedObject("nested_object"));
    }

}
