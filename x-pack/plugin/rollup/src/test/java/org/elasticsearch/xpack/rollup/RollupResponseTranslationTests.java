/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RollupResponseTranslationTests extends AggregatorTestCase {

    public void testLiveFailure() {
        MultiSearchResponse.Item[] failure = new MultiSearchResponse.Item[]{
                new MultiSearchResponse.Item(null, new RuntimeException("foo")),
                new MultiSearchResponse.Item(null, null)};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);

        Exception e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.combineResponses(failure,
                        new InternalAggregation.ReduceContext(bigArrays, scriptService, true)));
        assertThat(e.getMessage(), equalTo("foo"));

        e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.translateResponse(failure,
                        new InternalAggregation.ReduceContext(bigArrays, scriptService, true)));
        assertThat(e.getMessage(), equalTo("foo"));

        e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.verifyResponse(failure[0]));
        assertThat(e.getMessage(), equalTo("foo"));
    }

    public void testRollupFailure() {
        MultiSearchResponse.Item[] failure = new MultiSearchResponse.Item[]{
                new MultiSearchResponse.Item(null, new RuntimeException("rollup failure"))};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);

        Exception e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.translateResponse(failure,
                        new InternalAggregation.ReduceContext(bigArrays, scriptService, true)));
        assertThat(e.getMessage(), equalTo("rollup failure"));
    }

    public void testLiveMissingRollupMissing() {
        MultiSearchResponse.Item[] failure = new MultiSearchResponse.Item[]{
                new MultiSearchResponse.Item(null, new IndexNotFoundException("foo")),
                new MultiSearchResponse.Item(null, new IndexNotFoundException("foo"))};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
                () -> RollupResponseTranslator.combineResponses(failure,
                        new InternalAggregation.ReduceContext(bigArrays, scriptService, true)));
        assertThat(e.getMessage(), equalTo("Index [[foo]] was not found, likely because it was deleted while the request was in-flight. " +
            "Rollup does not support partial search results, please try the request again."));
    }

    public void testMissingLiveIndex() throws Exception {
        SearchResponse responseWithout = mock(SearchResponse.class);
        when(responseWithout.getTook()).thenReturn(new TimeValue(100));
        List<InternalAggregation> aggTree = new ArrayList<>(1);
        InternalFilter filter = mock(InternalFilter.class);

        List<InternalAggregation> subaggs = new ArrayList<>(2);
        Map<String, Object> metadata = new HashMap<>(1);
        metadata.put(RollupField.ROLLUP_META + "." + RollupField.COUNT_FIELD, "foo." + RollupField.COUNT_FIELD);
        InternalSum sum = mock(InternalSum.class);
        when(sum.getValue()).thenReturn(10.0);
        when(sum.value()).thenReturn(10.0);
        when(sum.getName()).thenReturn("foo");
        when(sum.getMetaData()).thenReturn(metadata);
        when(sum.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(sum);

        InternalSum count = mock(InternalSum.class);
        when(count.getValue()).thenReturn(2.0);
        when(count.value()).thenReturn(2.0);
        when(count.getName()).thenReturn("foo." + RollupField.COUNT_FIELD);
        when(count.getMetaData()).thenReturn(null);
        when(count.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(count);

        when(filter.getAggregations()).thenReturn(new InternalAggregations(subaggs));
        when(filter.getName()).thenReturn("filter_foo");
        aggTree.add(filter);

        Aggregations mockAggsWithout = new InternalAggregations(aggTree);
        when(responseWithout.getAggregations()).thenReturn(mockAggsWithout);

        MultiSearchResponse.Item[] msearch = new MultiSearchResponse.Item[]{
                new MultiSearchResponse.Item(null, new IndexNotFoundException("foo")),
                new MultiSearchResponse.Item(responseWithout, null)};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> RollupResponseTranslator.combineResponses(msearch,
                new InternalAggregation.ReduceContext(bigArrays, scriptService, true)));
        assertThat(e.getMessage(), equalTo("Index [[foo]] was not found, likely because it was deleted while the request was in-flight. " +
            "Rollup does not support partial search results, please try the request again."));
    }

    public void testRolledMissingAggs() throws Exception {
        SearchResponse responseWithout = mock(SearchResponse.class);
        when(responseWithout.getTook()).thenReturn(new TimeValue(100));

        Aggregations mockAggsWithout = new InternalAggregations(Collections.emptyList());
        when(responseWithout.getAggregations()).thenReturn(mockAggsWithout);

        MultiSearchResponse.Item[] msearch = new MultiSearchResponse.Item[]{
                new MultiSearchResponse.Item(responseWithout, null)};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);

        SearchResponse response = RollupResponseTranslator.translateResponse(msearch,
            new InternalAggregation.ReduceContext(bigArrays, scriptService, true));
        assertNotNull(response);
        Aggregations responseAggs = response.getAggregations();
        assertThat(responseAggs.asList().size(), equalTo(0));
    }

    public void testMissingRolledIndex() {
        SearchResponse response = mock(SearchResponse.class);

        MultiSearchResponse.Item[] msearch = new MultiSearchResponse.Item[]{
                new MultiSearchResponse.Item(response, null),
                new MultiSearchResponse.Item(null, new IndexNotFoundException("foo"))};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> RollupResponseTranslator.combineResponses(msearch,
            new InternalAggregation.ReduceContext(bigArrays, scriptService, true)));
        assertThat(e.getMessage(), equalTo("Index [[foo]] was not found, likely because it was deleted while the request was in-flight. " +
            "Rollup does not support partial search results, please try the request again."));
    }

    public void testVerifyNormal() throws Exception {
        SearchResponse response = mock(SearchResponse.class);
        MultiSearchResponse.Item item = new MultiSearchResponse.Item(response, null);

        SearchResponse finalResponse = RollupResponseTranslator.verifyResponse(item);
        assertThat(finalResponse, equalTo(response));
    }

    public void testVerifyMissingNormal() {
        MultiSearchResponse.Item missing = new MultiSearchResponse.Item(null, new IndexNotFoundException("foo"));
        Exception e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.verifyResponse(missing));
        assertThat(e.getMessage(), equalTo("no such index [foo]"));
    }

    public void testTranslateRollup() throws Exception {
        SearchResponse response = mock(SearchResponse.class);
        when(response.getTook()).thenReturn(new TimeValue(100));
        List<InternalAggregation> aggTree = new ArrayList<>(1);
        InternalFilter filter = mock(InternalFilter.class);

        List<InternalAggregation> subaggs = new ArrayList<>(2);
        Map<String, Object> metadata = new HashMap<>(1);
        metadata.put(RollupField.ROLLUP_META + "." + RollupField.COUNT_FIELD, "foo." + RollupField.COUNT_FIELD);
        InternalSum sum = mock(InternalSum.class);
        when(sum.getValue()).thenReturn(10.0);
        when(sum.value()).thenReturn(10.0);
        when(sum.getName()).thenReturn("foo");
        when(sum.getMetaData()).thenReturn(metadata);
        when(sum.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(sum);

        InternalSum count = mock(InternalSum.class);
        when(count.getValue()).thenReturn(2.0);
        when(count.value()).thenReturn(2.0);
        when(count.getName()).thenReturn("foo." + RollupField.COUNT_FIELD);
        when(count.getMetaData()).thenReturn(null);
        when(count.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(count);

        when(filter.getAggregations()).thenReturn(new InternalAggregations(subaggs));
        when(filter.getName()).thenReturn("filter_foo");
        aggTree.add(filter);

        Aggregations mockAggs = new InternalAggregations(aggTree);
        when(response.getAggregations()).thenReturn(mockAggs);
        MultiSearchResponse.Item item = new MultiSearchResponse.Item(response, null);

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);
        InternalAggregation.ReduceContext context = new InternalAggregation.ReduceContext(bigArrays, scriptService, true);

        SearchResponse finalResponse = RollupResponseTranslator.translateResponse(new MultiSearchResponse.Item[]{item}, context);
        assertNotNull(finalResponse);
        Aggregations responseAggs = finalResponse.getAggregations();
        assertNotNull(finalResponse);
        Avg avg = responseAggs.get("foo");
        assertThat(avg.getValue(), equalTo(5.0));
    }

    public void testTranslateMissingRollup() {
        MultiSearchResponse.Item missing = new MultiSearchResponse.Item(null, new IndexNotFoundException("foo"));
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);
        InternalAggregation.ReduceContext context = new InternalAggregation.ReduceContext(bigArrays, scriptService, true);

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
                () -> RollupResponseTranslator.translateResponse(new MultiSearchResponse.Item[]{missing}, context));
        assertThat(e.getMessage(), equalTo("Index [foo] was not found, likely because it was deleted while the request was in-flight. " +
            "Rollup does not support partial search results, please try the request again."));
    }

    public void testMissingFilter() {
        SearchResponse protoResponse = mock(SearchResponse.class);
        List<InternalAggregation> protoAggTree = new ArrayList<>(1);
        InternalMax protoMax = mock(InternalMax.class);
        when(protoMax.getName()).thenReturn("foo");
        protoAggTree.add(protoMax);
        Aggregations protoMockAggs = new InternalAggregations(protoAggTree);
        when(protoResponse.getAggregations()).thenReturn(protoMockAggs);
        MultiSearchResponse.Item unrolledResponse = new MultiSearchResponse.Item(protoResponse, null);

        SearchResponse responseWithout = mock(SearchResponse.class);
        List<InternalAggregation> aggTreeWithoutFilter = new ArrayList<>(1);
        InternalMax max = mock(InternalMax.class);
        when(max.getName()).thenReturn("bizzbuzz");
        aggTreeWithoutFilter.add(max);
        Aggregations mockAggsWithout = new InternalAggregations(aggTreeWithoutFilter);
        when(responseWithout.getAggregations()).thenReturn(mockAggsWithout);
        MultiSearchResponse.Item rolledResponse = new MultiSearchResponse.Item(responseWithout, null);

        MultiSearchResponse.Item[] msearch = new MultiSearchResponse.Item[]{unrolledResponse, rolledResponse};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);

        Exception e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.combineResponses(msearch,
                        new InternalAggregation.ReduceContext(bigArrays, scriptService, true)));
        assertThat(e.getMessage(), containsString("Expected [bizzbuzz] to be a FilterAggregation"));
    }

    public void testMatchingNameNotFilter() {
        SearchResponse protoResponse = mock(SearchResponse.class);
        List<InternalAggregation> protoAggTree = new ArrayList<>(1);
        InternalMax protoMax = mock(InternalMax.class);
        when(protoMax.getName()).thenReturn("foo");
        protoAggTree.add(protoMax);
        Aggregations protoMockAggs = new InternalAggregations(protoAggTree);
        when(protoResponse.getAggregations()).thenReturn(protoMockAggs);
        MultiSearchResponse.Item unrolledResponse = new MultiSearchResponse.Item(protoResponse, null);

        SearchResponse responseWithout = mock(SearchResponse.class);
        List<InternalAggregation> aggTreeWithoutFilter = new ArrayList<>(1);
        InternalMax max = new InternalMax("filter_foo", 0, DocValueFormat.RAW, Collections.emptyList(), null);
        aggTreeWithoutFilter.add(max);
        Aggregations mockAggsWithout = new InternalAggregations(aggTreeWithoutFilter);
        when(responseWithout.getAggregations()).thenReturn(mockAggsWithout);
        MultiSearchResponse.Item rolledResponse = new MultiSearchResponse.Item(responseWithout, null);

        MultiSearchResponse.Item[] msearch = new MultiSearchResponse.Item[]{unrolledResponse, rolledResponse};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);

        Exception e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.combineResponses(msearch,
                        new InternalAggregation.ReduceContext(bigArrays, scriptService, true)));
        assertThat(e.getMessage(),
                equalTo("Expected [filter_foo] to be a FilterAggregation, but was [InternalMax]"));
    }

    public void testSimpleReduction() throws Exception {
        SearchResponse protoResponse = mock(SearchResponse.class);
        when(protoResponse.getTook()).thenReturn(new TimeValue(100));
        List<InternalAggregation> protoAggTree = new ArrayList<>(1);
        InternalAvg internalAvg = new InternalAvg("foo", 10, 2, DocValueFormat.RAW, Collections.emptyList(), null);
        protoAggTree.add(internalAvg);
        Aggregations protoMockAggs = new InternalAggregations(protoAggTree);
        when(protoResponse.getAggregations()).thenReturn(protoMockAggs);
        MultiSearchResponse.Item unrolledResponse = new MultiSearchResponse.Item(protoResponse, null);

        SearchResponse responseWithout = mock(SearchResponse.class);
        when(responseWithout.getTook()).thenReturn(new TimeValue(100));
        List<InternalAggregation> aggTree = new ArrayList<>(1);
        InternalFilter filter = mock(InternalFilter.class);

        List<InternalAggregation> subaggs = new ArrayList<>(2);
        Map<String, Object> metadata = new HashMap<>(1);
        metadata.put(RollupField.ROLLUP_META + "." + RollupField.COUNT_FIELD, "foo." + RollupField.COUNT_FIELD);
        InternalSum sum = mock(InternalSum.class);
        when(sum.getValue()).thenReturn(10.0);
        when(sum.value()).thenReturn(10.0);
        when(sum.getName()).thenReturn("foo");
        when(sum.getMetaData()).thenReturn(metadata);
        when(sum.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(sum);

        InternalSum count = mock(InternalSum.class);
        when(count.getValue()).thenReturn(2.0);
        when(count.value()).thenReturn(2.0);
        when(count.getName()).thenReturn("foo." + RollupField.COUNT_FIELD);
        when(count.getMetaData()).thenReturn(null);
        when(count.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(count);

        when(filter.getAggregations()).thenReturn(new InternalAggregations(subaggs));
        when(filter.getName()).thenReturn("filter_foo");
        aggTree.add(filter);

        Aggregations mockAggsWithout = new InternalAggregations(aggTree);
        when(responseWithout.getAggregations()).thenReturn(mockAggsWithout);
        MultiSearchResponse.Item rolledResponse = new MultiSearchResponse.Item(responseWithout, null);

        MultiSearchResponse.Item[] msearch = new MultiSearchResponse.Item[]{unrolledResponse, rolledResponse};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);


        SearchResponse response = RollupResponseTranslator.combineResponses(msearch,
                new InternalAggregation.ReduceContext(bigArrays, scriptService, true));
        assertNotNull(response);
        Aggregations responseAggs = response.getAggregations();
        assertNotNull(responseAggs);
        Avg avg = responseAggs.get("foo");
        assertThat(avg.getValue(), equalTo(5.0));
    }

    public void testUnsupported() throws IOException {

        GeoBoundsAggregationBuilder geo1
                = new GeoBoundsAggregationBuilder("foo").field("bar");
        GeoBoundsAggregationBuilder geo2
                = new GeoBoundsAggregationBuilder("foo").field("bar");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            }, geo1,
            iw -> {
                iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            }, geo2,
                new MappedFieldType[]{fieldType}, new MappedFieldType[]{fieldType});

        Exception e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 0));
        assertThat(e.getMessage(), equalTo("Unable to unroll aggregation tree.  " +
                "Aggregation [foo] is of type [InternalGeoBounds] which is currently unsupported."));
    }

    public void testUnsupportedMultiBucket() throws IOException {

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType();
        fieldType.setHasDocValues(true);
        fieldType.setIndexOptions(IndexOptions.DOCS);
        fieldType.setName("foo");
        QueryBuilder filter = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("field", "foo"))
                .should(QueryBuilders.termQuery("field", "bar"));
        SignificantTermsAggregationBuilder builder = new SignificantTermsAggregationBuilder(
                "test", ValueType.STRING)
                .field("field")
                .backgroundFilter(filter);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, builder,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 1));
                    iw.addDocument(timestampedValueRollupDoc(200, 2));
                    iw.addDocument(timestampedValueRollupDoc(300, 3));
                }, builder,
                new MappedFieldType[]{fieldType}, new MappedFieldType[]{fieldType});


        Exception e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 0));
        assertThat(e.getMessage(), equalTo("Unable to unroll aggregation tree.  Aggregation [test] is of type " +
                "[UnmappedSignificantTerms] which is currently unsupported."));
    }

    public void testMismatch() throws IOException {
        GeoBoundsAggregationBuilder geoBoundsAggregationBuilder
                = new GeoBoundsAggregationBuilder("histo").field("bar");

        DateHistogramAggregationBuilder histoBuilder = new DateHistogramAggregationBuilder("histo")
                .field("bar").fixedInterval(new DateHistogramInterval("100ms"));
        FilterAggregationBuilder filterBuilder = new FilterAggregationBuilder("filter", new TermQueryBuilder("foo", "bar"));
        filterBuilder.subAggregation(histoBuilder);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
                    iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
                    iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
                }, geoBoundsAggregationBuilder,
                iw -> {
                    iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
                    iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
                    iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
                }, filterBuilder,
                new MappedFieldType[]{fieldType}, new MappedFieldType[]{fieldType});

        // TODO SearchResponse.Clusters is not public, using null for now.  Should fix upstream.
        MultiSearchResponse.Item unrolledItem = new MultiSearchResponse.Item(new SearchResponse(
                new InternalSearchResponse(null,
                        new InternalAggregations(Collections.singletonList(responses.get(0))), null, null, false, false, 1),
                        null, 1, 1, 0, 10, null, null), null);
        MultiSearchResponse.Item rolledItem = new MultiSearchResponse.Item(new SearchResponse(
                new InternalSearchResponse(null,
                        new InternalAggregations(Collections.singletonList(responses.get(1))), null, null, false, false, 1),
                        null, 1, 1, 0, 10, null, null), null);

        MultiSearchResponse.Item[] msearch = new MultiSearchResponse.Item[]{unrolledItem, rolledItem};

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);
        InternalAggregation.ReduceContext reduceContext = new InternalAggregation.ReduceContext(bigArrays, scriptService, true);
        ClassCastException e = expectThrows(ClassCastException.class,
                () -> RollupResponseTranslator.combineResponses(msearch, reduceContext));
        assertThat(e.getMessage(),
            containsString("org.elasticsearch.search.aggregations.metrics.InternalGeoBounds"));
        assertThat(e.getMessage(),
            containsString("org.elasticsearch.search.aggregations.InternalMultiBucketAggregation"));
    }

    public void testDateHisto() throws IOException {
        DateHistogramAggregationBuilder nonRollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp").fixedInterval(new DateHistogramInterval("100ms"));

        DateHistogramAggregationBuilder rollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp.date_histogram." + RollupField.TIMESTAMP)
                .fixedInterval(new DateHistogramInterval("100ms"))
                .subAggregation(new SumAggregationBuilder("histo." + RollupField.COUNT_FIELD)
                        .field("timestamp.date_histogram." + RollupField.COUNT_FIELD));

        DateFieldMapper.Builder nrBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType nrFTtimestamp = nrBuilder.fieldType();
        nrFTtimestamp.setHasDocValues(true);
        nrFTtimestamp.setName(nonRollupHisto.field());

        DateFieldMapper.Builder rBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType rFTtimestamp = rBuilder.fieldType();
        rFTtimestamp.setHasDocValues(true);
        rFTtimestamp.setName(rollupHisto.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("histo." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("timestamp.date_histogram." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, nonRollupHisto,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 1));
                    iw.addDocument(timestampedValueRollupDoc(200, 2));
                    iw.addDocument(timestampedValueRollupDoc(300, 3));
                }, rollupHisto,
                new MappedFieldType[]{nrFTtimestamp}, new MappedFieldType[]{rFTtimestamp, rFTvalue});

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 0);
        assertThat(unrolled.toString(), equalTo(responses.get(0).toString()));
        assertThat(unrolled.toString(), not(equalTo(responses.get(1).toString())));
    }

    public void testDateHistoWithGap() throws IOException {
        DateHistogramAggregationBuilder nonRollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp").fixedInterval(new DateHistogramInterval("100ms"))
                .minDocCount(0);

        DateHistogramAggregationBuilder rollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp.date_histogram." + RollupField.TIMESTAMP)
                .fixedInterval(new DateHistogramInterval("100ms"))
                .minDocCount(0)
                .subAggregation(new SumAggregationBuilder("histo." + RollupField.COUNT_FIELD)
                        .field("timestamp.date_histogram." + RollupField.COUNT_FIELD));

        DateFieldMapper.Builder nrBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType nrFTtimestamp = nrBuilder.fieldType();
        nrFTtimestamp.setHasDocValues(true);
        nrFTtimestamp.setName(nonRollupHisto.field());

        DateFieldMapper.Builder rBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType rFTtimestamp = rBuilder.fieldType();
        rFTtimestamp.setHasDocValues(true);
        rFTtimestamp.setName(rollupHisto.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("histo." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("timestamp.date_histogram." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(400, 3));
                }, nonRollupHisto,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 1));
                    iw.addDocument(timestampedValueRollupDoc(200, 2));
                    iw.addDocument(timestampedValueRollupDoc(400, 3));
                }, rollupHisto,
                new MappedFieldType[]{nrFTtimestamp}, new MappedFieldType[]{rFTtimestamp, rFTvalue});



        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 0);

        // Reduce the InternalDateHistogram response so we can fill buckets
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService scriptService = mock(ScriptService.class);
        InternalAggregation.ReduceContext context = new InternalAggregation.ReduceContext(bigArrays, scriptService, true);

        InternalAggregation reduced = ((InternalDateHistogram)unrolled).reduce(Collections.singletonList(unrolled), context);
        assertThat(reduced.toString(), equalTo("{\"histo\":{\"buckets\":[{\"key_as_string\":\"1970-01-01T00:00:00.100Z\",\"key\":100," +
                "\"doc_count\":1},{\"key_as_string\":\"1970-01-01T00:00:00.200Z\",\"key\":200,\"doc_count\":1}," +
                "{\"key_as_string\":\"1970-01-01T00:00:00.300Z\",\"key\":300,\"doc_count\":0,\"histo._count\":{\"value\":0.0}}," +
                "{\"key_as_string\":\"1970-01-01T00:00:00.400Z\",\"key\":400,\"doc_count\":1}]}}"));
    }

    public void testNonMatchingPartition() throws IOException {
        DateHistogramAggregationBuilder nonRollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp").fixedInterval(new DateHistogramInterval("100ms"))
                .minDocCount(0);

        DateHistogramAggregationBuilder rollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp.date_histogram." + RollupField.TIMESTAMP)
                .fixedInterval(new DateHistogramInterval("100ms"))
                .minDocCount(0)
                .subAggregation(new SumAggregationBuilder("histo." + RollupField.COUNT_FIELD)
                        .field("timestamp.date_histogram." + RollupField.COUNT_FIELD));

        DateFieldMapper.Builder nrBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType nrFTtimestamp = nrBuilder.fieldType();
        nrFTtimestamp.setHasDocValues(true);
        nrFTtimestamp.setName(nonRollupHisto.field());

        DateFieldMapper.Builder rBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType rFTtimestamp = rBuilder.fieldType();
        rFTtimestamp.setHasDocValues(true);
        rFTtimestamp.setName(rollupHisto.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("histo." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("timestamp.date_histogram." + RollupField.COUNT_FIELD);

        KeywordFieldMapper.Builder nrKeywordBuilder = new KeywordFieldMapper.Builder("partition");
        KeywordFieldMapper.KeywordFieldType nrKeywordFT = nrKeywordBuilder.fieldType();
        nrKeywordFT.setHasDocValues(true);
        nrKeywordFT.setName("partition");

        KeywordFieldMapper.Builder rKeywordBuilder = new KeywordFieldMapper.Builder("partition");
        KeywordFieldMapper.KeywordFieldType rKeywordFT = rKeywordBuilder.fieldType();
        rKeywordFT.setHasDocValues(true);
        rKeywordFT.setName("partition");

        // Note: term query for "a"
        List<InternalAggregation> results = new ArrayList<>(2);
        results.add(doQuery(new TermQuery(new Term("partition", "a")), iw -> {
            // Time 100: Two "a" documents, one "b" doc
            Document doc = new Document();
            doc.add(new SortedNumericDocValuesField("timestamp", 100));
            doc.add(new TextField("partition", "a", Field.Store.NO));
            iw.addDocument(doc);
            doc = new Document();
            doc.add(new SortedNumericDocValuesField("timestamp", 100));
            doc.add(new TextField("partition", "a", Field.Store.NO));
            iw.addDocument(doc);
            doc = new Document();
            doc.add(new SortedNumericDocValuesField("timestamp", 100));
            doc.add(new TextField("partition", "b", Field.Store.NO));
            iw.addDocument(doc);

            // Time 200: one "a" document, one "b" doc
            doc = new Document();
            doc.add(new SortedNumericDocValuesField("timestamp", 200));
            doc.add(new TextField("partition", "a", Field.Store.NO));
            iw.addDocument(doc);
            doc = new Document();
            doc.add(new SortedNumericDocValuesField("timestamp", 200));
            doc.add(new TextField("partition", "b", Field.Store.NO));
            iw.addDocument(doc);

        }, nonRollupHisto, new MappedFieldType[]{nrFTtimestamp, nrKeywordFT}));

        // Note: term query for "a"
        results.add(doQuery(new TermQuery(new Term("partition.terms." + RollupField.VALUE, "a")),
                iw -> {
                    // Time 100: Two "a" documents, one "b" doc
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, 100));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 2));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
                    doc.add(new TextField("partition.terms." + RollupField.VALUE, "a", Field.Store.NO));
                    doc.add(new SortedNumericDocValuesField("partition.terms." + RollupField.COUNT_FIELD, 2));
                    iw.addDocument(doc);
                    doc = new Document();
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, 100));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 1));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
                    doc.add(new TextField("partition.terms." + RollupField.VALUE, "b", Field.Store.NO));
                    doc.add(new SortedNumericDocValuesField("partition.terms." + RollupField.COUNT_FIELD, 1));
                    iw.addDocument(doc);

                    // Time 200: one "a" document, one "b" doc
                    doc = new Document();
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, 200));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 1));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
                    doc.add(new TextField("partition.terms." + RollupField.VALUE, "a", Field.Store.NO));
                    doc.add(new SortedNumericDocValuesField("partition.terms." + RollupField.COUNT_FIELD, 1));
                    iw.addDocument(doc);
                    doc = new Document();
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, 200));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 1));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
                    doc.add(new TextField("partition.terms." + RollupField.VALUE, "b", Field.Store.NO));
                    doc.add(new SortedNumericDocValuesField("partition.terms." + RollupField.COUNT_FIELD, 1));
                    iw.addDocument(doc);
                }, rollupHisto, new MappedFieldType[]{rFTtimestamp, rFTvalue, rKeywordFT}));

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(results.get(1), null, null, 0);
        assertThat(((InternalDateHistogram)unrolled).getBuckets().size(), equalTo(2));
        assertThat(((InternalDateHistogram)unrolled).getBuckets().get(0).getDocCount(), equalTo(2L)); // two "a" at 100
        assertThat(((InternalDateHistogram)unrolled).getBuckets().get(1).getDocCount(), equalTo(1L)); // one "a" at 200
        assertThat(((InternalDateHistogram)unrolled).getBuckets().get(0).getKeyAsString(), equalTo("1970-01-01T00:00:00.100Z"));
        assertThat(unrolled.toString(), equalTo("{\"histo\":{\"buckets\":[{\"key_as_string\":\"1970-01-01T00:00:00.100Z\"," +
                "\"key\":100,\"doc_count\":2},{\"key_as_string\":\"1970-01-01T00:00:00.200Z\",\"key\":200,\"doc_count\":1}]}}"));
        assertThat(unrolled.toString(), not(equalTo(results.get(1).toString())));
    }

    public void testDateHistoOverlappingAggTrees() throws IOException {
        DateHistogramAggregationBuilder nonRollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp").fixedInterval(new DateHistogramInterval("100ms"));

        DateHistogramAggregationBuilder rollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp.date_histogram." + RollupField.TIMESTAMP)
                .fixedInterval(new DateHistogramInterval("100ms"))
                .subAggregation(new SumAggregationBuilder("histo." + RollupField.COUNT_FIELD)
                        .field("timestamp.date_histogram." + RollupField.COUNT_FIELD));

        DateFieldMapper.Builder nrBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType nrFTtimestamp = nrBuilder.fieldType();
        nrFTtimestamp.setHasDocValues(true);
        nrFTtimestamp.setName(nonRollupHisto.field());

        DateFieldMapper.Builder rBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType rFTtimestamp = rBuilder.fieldType();
        rFTtimestamp.setHasDocValues(true);
        rFTtimestamp.setName(rollupHisto.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("histo." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("timestamp.date_histogram." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, nonRollupHisto,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 1));
                    iw.addDocument(timestampedValueRollupDoc(200, 2));
                    iw.addDocument(timestampedValueRollupDoc(300, 3));
                }, rollupHisto,
                new MappedFieldType[]{nrFTtimestamp}, new MappedFieldType[]{rFTtimestamp, rFTvalue});

        List<InternalAggregation> currentTree = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                }, nonRollupHisto,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 1));
                    iw.addDocument(timestampedValueRollupDoc(200, 2));
                }, rollupHisto,
                new MappedFieldType[]{nrFTtimestamp}, new MappedFieldType[]{rFTtimestamp, rFTvalue});

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, currentTree.get(1), 0);

        // Times 100/200 overlap with currentTree, so doc_count will be zero
        assertThat(((Object[])unrolled.getProperty("_count"))[0], equalTo(0L));
        assertThat(((Object[])unrolled.getProperty("_count"))[1], equalTo(0L));

        // This time (300) was not in the currentTree so it will have a doc_count of one
        assertThat(((Object[])unrolled.getProperty("_count"))[2], equalTo(1L));

    }

    public void testDateHistoOverlappingMergeRealIntoZero() throws IOException {
        DateHistogramAggregationBuilder nonRollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp").fixedInterval(new DateHistogramInterval("100ms"));

        DateHistogramAggregationBuilder rollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp.date_histogram." + RollupField.TIMESTAMP)
                .fixedInterval(new DateHistogramInterval("100ms"))
                .subAggregation(new SumAggregationBuilder("histo." + RollupField.COUNT_FIELD)
                        .field("timestamp.date_histogram." + RollupField.COUNT_FIELD));

        DateFieldMapper.Builder nrBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType nrFTtimestamp = nrBuilder.fieldType();
        nrFTtimestamp.setHasDocValues(true);
        nrFTtimestamp.setName(nonRollupHisto.field());

        DateFieldMapper.Builder rBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType rFTtimestamp = rBuilder.fieldType();
        rFTtimestamp.setHasDocValues(true);
        rFTtimestamp.setName(rollupHisto.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("histo." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("timestamp.date_histogram." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, nonRollupHisto,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 1));
                    iw.addDocument(timestampedValueRollupDoc(200, 2));
                    iw.addDocument(timestampedValueRollupDoc(300, 3));
                }, rollupHisto,
                new MappedFieldType[]{nrFTtimestamp}, new MappedFieldType[]{rFTtimestamp, rFTvalue});

        InternalAggregation currentTree = doQuery(new MatchAllDocsQuery(),
                iw -> {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, 100));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 0));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
                    iw.addDocument(doc);

                    Document doc2 = new Document();
                    doc2.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, 200));
                    doc2.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 0));
                    doc2.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
                    iw.addDocument(doc2);

                }, rollupHisto, new MappedFieldType[]{rFTtimestamp, rFTvalue});

        // In this test we merge real buckets into zero count buckets (e.g. empty list of buckets after unrolling)
        InternalAggregation unrolledCurrentTree = RollupResponseTranslator.unrollAgg(currentTree, null, null, 0);
        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, unrolledCurrentTree, 0);

        // Times 100/200 overlap with currentTree, but doc_count was zero, so returned doc_count should be one
        assertThat(((Object[])unrolled.getProperty("_count"))[0], equalTo(1L));
        assertThat(((Object[])unrolled.getProperty("_count"))[1], equalTo(1L));

        // This time (300) was not in the currentTree so it will have a doc_count of one
        assertThat(((Object[])unrolled.getProperty("_count"))[2], equalTo(1L));
    }

    public void testDateHistoOverlappingMergeZeroIntoReal() throws IOException {
        DateHistogramAggregationBuilder nonRollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp").fixedInterval(new DateHistogramInterval("100ms")).minDocCount(0);

        DateHistogramAggregationBuilder rollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp.date_histogram." + RollupField.TIMESTAMP)
                .fixedInterval(new DateHistogramInterval("100ms"))
                .minDocCount(0)
                .subAggregation(new SumAggregationBuilder("histo." + RollupField.COUNT_FIELD)
                        .field("timestamp.date_histogram." + RollupField.COUNT_FIELD));

        DateFieldMapper.Builder nrBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType nrFTtimestamp = nrBuilder.fieldType();
        nrFTtimestamp.setHasDocValues(true);
        nrFTtimestamp.setName(nonRollupHisto.field());

        DateFieldMapper.Builder rBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType rFTtimestamp = rBuilder.fieldType();
        rFTtimestamp.setHasDocValues(true);
        rFTtimestamp.setName(rollupHisto.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("histo." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("timestamp.date_histogram." + RollupField.COUNT_FIELD);

        InternalAggregation currentTree = doQuery(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 1));
                    iw.addDocument(timestampedValueRollupDoc(200, 2));
                    iw.addDocument(timestampedValueRollupDoc(300, 3));
                }, rollupHisto, new MappedFieldType[]{rFTtimestamp, rFTvalue});

        InternalAggregation responses = doQuery(new MatchAllDocsQuery(),
                iw -> {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, 100));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 0));
                    doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
                    iw.addDocument(doc);

                    Document doc2 = new Document();
                    doc2.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, 200));
                    doc2.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 0));
                    doc2.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
                    iw.addDocument(doc2);

                }, rollupHisto, new MappedFieldType[]{rFTtimestamp, rFTvalue});


        // In this test, we merge zero_count buckets into existing buckets to ensure the metrics remain
        InternalAggregation unrolledCurrentTree = RollupResponseTranslator.unrollAgg(currentTree, null, null, 0);
        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses, null, unrolledCurrentTree, 0);

        // All values overlap and were zero counts themselves, so the unrolled response should be empty list of buckets
        assertThat(((InternalDateHistogram)unrolled).getBuckets().size(), equalTo(0));
    }

    public void testAvg() throws IOException {
        AvgAggregationBuilder nonRollup = new AvgAggregationBuilder("avg")
                .field("foo");

        SumAggregationBuilder rollup = new SumAggregationBuilder("avg")
                .field("foo.avg." + RollupField.VALUE);

        NumberFieldMapper.Builder nrValueMapper = new NumberFieldMapper.Builder("foo",
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType nrFTvalue = nrValueMapper.fieldType();
        nrFTvalue.setHasDocValues(true);
        nrFTvalue.setName("foo");

        NumberFieldMapper.Builder rValueMapper = new NumberFieldMapper.Builder("avg.foo",
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = rValueMapper.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("foo.avg." + RollupField.VALUE);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, nonRollup,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 6));
                }, rollup,
                new MappedFieldType[]{nrFTvalue}, new MappedFieldType[]{rFTvalue});

        // NOTE: we manually set the count to 3 here, which is somewhat cheating.  Will have to rely on
        // other tests to verify that the avg's count is set correctly
        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 3);
        assertThat(unrolled.toString(), equalTo(responses.get(0).toString()));
        assertThat(unrolled.toString(), not(equalTo(responses.get(1).toString())));
    }

    public void testMetric() throws IOException {
        int i = randomIntBetween(0, 2);
        AggregationBuilder nonRollup = null;
        AggregationBuilder rollup = null;
        final long rollupValue;
        String fieldName = null;

        if (i == 0) {
            nonRollup = new MaxAggregationBuilder("test_metric").field("foo");
            fieldName = "foo.max." + RollupField.VALUE;
            rollup = new MaxAggregationBuilder("test_metric").field(fieldName);
            rollupValue = 3;
        } else if (i == 1) {
            nonRollup = new MinAggregationBuilder("test_metric").field("foo");
            fieldName = "foo.min." + RollupField.VALUE;
            rollup = new MinAggregationBuilder("test_metric").field(fieldName);
            rollupValue = 1;
        } else if (i == 2) {
            nonRollup = new SumAggregationBuilder("test_metric").field("foo");
            fieldName = "foo.sum." + RollupField.VALUE;
            rollup = new SumAggregationBuilder("test_metric").field(fieldName);
            rollupValue = 6;
        } else {
            rollupValue = 0;
        }

        NumberFieldMapper.Builder nrValueMapper = new NumberFieldMapper.Builder("foo",
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType nrFTvalue = nrValueMapper.fieldType();
        nrFTvalue.setHasDocValues(true);
        nrFTvalue.setName("foo");

        NumberFieldMapper.Builder rValueMapper = new NumberFieldMapper.Builder("foo",
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = rValueMapper.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName(fieldName);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, nonRollup,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, rollupValue));
                }, rollup,
                new MappedFieldType[]{nrFTvalue}, new MappedFieldType[]{rFTvalue});

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 1);
        assertThat(unrolled.toString(), equalTo(responses.get(0).toString()));
    }

    public void testUnsupportedMetric() throws IOException {


        AggregationBuilder nonRollup = new CardinalityAggregationBuilder("test_metric", ValueType.LONG).field("foo");
        String fieldName = "foo.max." + RollupField.VALUE;
        AggregationBuilder rollup = new CardinalityAggregationBuilder("test_metric", ValueType.LONG).field(fieldName);

        NumberFieldMapper.Builder nrValueMapper = new NumberFieldMapper.Builder("foo",
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType nrFTvalue = nrValueMapper.fieldType();
        nrFTvalue.setHasDocValues(true);
        nrFTvalue.setName("foo");

        NumberFieldMapper.Builder rValueMapper = new NumberFieldMapper.Builder("foo",
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = rValueMapper.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName(fieldName);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, nonRollup,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 3));
                }, rollup,
                new MappedFieldType[]{nrFTvalue}, new MappedFieldType[]{rFTvalue});

        RuntimeException e = expectThrows(RuntimeException.class,
                () -> RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 1));
        assertThat(e.getMessage(), equalTo("Unable to unroll metric.  Aggregation [test_metric] is of type " +
                "[InternalCardinality] which is currently unsupported."));
    }

    public void testStringTerms() throws IOException {
        TermsAggregationBuilder nonRollupTerms = new TermsAggregationBuilder("terms", ValueType.STRING)
                .field("stringField");

        TermsAggregationBuilder rollupTerms = new TermsAggregationBuilder("terms", ValueType.STRING)
                .field("stringfield.terms." + RollupField.VALUE)
                .subAggregation(new SumAggregationBuilder("terms." + RollupField.COUNT_FIELD)
                        .field("stringfield.terms." + RollupField.COUNT_FIELD));

        KeywordFieldMapper.Builder nrBuilder = new KeywordFieldMapper.Builder("terms");
        KeywordFieldMapper.KeywordFieldType nrFTterm = nrBuilder.fieldType();
        nrFTterm.setHasDocValues(true);
        nrFTterm.setName(nonRollupTerms.field());

        KeywordFieldMapper.Builder rBuilder = new KeywordFieldMapper.Builder("terms");
        KeywordFieldMapper.KeywordFieldType rFTterm = rBuilder.fieldType();
        rFTterm.setHasDocValues(true);
        rFTterm.setName(rollupTerms.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("terms." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("stringfield.terms." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(stringValueDoc("abc"));
                    iw.addDocument(stringValueDoc("abc"));
                    iw.addDocument(stringValueDoc("abc"));
                }, nonRollupTerms,
                iw -> {
                    iw.addDocument(stringValueRollupDoc("abc", 3));
                }, rollupTerms,
                new MappedFieldType[]{nrFTterm}, new MappedFieldType[]{rFTterm, rFTvalue});

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 0);
        assertThat(unrolled.toString(), equalTo(responses.get(0).toString()));
        assertThat(unrolled.toString(), not(equalTo(responses.get(1).toString())));
    }

    public void testStringTermsNullValue() throws IOException {
        TermsAggregationBuilder nonRollupTerms = new TermsAggregationBuilder("terms", ValueType.STRING)
            .field("stringField");

        TermsAggregationBuilder rollupTerms = new TermsAggregationBuilder("terms", ValueType.STRING)
            .field("stringfield.terms." + RollupField.VALUE)
            .subAggregation(new SumAggregationBuilder("terms." + RollupField.COUNT_FIELD)
                .field("stringfield.terms." + RollupField.COUNT_FIELD));

        KeywordFieldMapper.Builder nrBuilder = new KeywordFieldMapper.Builder("terms");
        KeywordFieldMapper.KeywordFieldType nrFTterm = nrBuilder.fieldType();
        nrFTterm.setHasDocValues(true);
        nrFTterm.setName(nonRollupTerms.field());

        KeywordFieldMapper.Builder rBuilder = new KeywordFieldMapper.Builder("terms");
        KeywordFieldMapper.KeywordFieldType rFTterm = rBuilder.fieldType();
        rFTterm.setHasDocValues(true);
        rFTterm.setName(rollupTerms.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("terms." + RollupField.COUNT_FIELD,
            NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("stringfield.terms." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(stringValueDoc("abc"));
                iw.addDocument(stringValueDoc("abc"));
                iw.addDocument(stringValueDoc("abc"));

                // off target
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("otherField", new BytesRef("other")));
                iw.addDocument(doc);
            }, nonRollupTerms,
            iw -> {
                iw.addDocument(stringValueRollupDoc("abc", 3));
            }, rollupTerms,
            new MappedFieldType[]{nrFTterm}, new MappedFieldType[]{rFTterm, rFTvalue});

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 0);

        // The null_value placeholder should be removed from the response and not visible here
        assertThat(unrolled.toString(), equalTo(responses.get(0).toString()));
        assertThat(unrolled.toString(), not(equalTo(responses.get(1).toString())));
    }

    public void testLongTerms() throws IOException {
        TermsAggregationBuilder nonRollupTerms = new TermsAggregationBuilder("terms", ValueType.LONG)
                .field("longField");

        TermsAggregationBuilder rollupTerms = new TermsAggregationBuilder("terms", ValueType.LONG)
                .field("longfield.terms." + RollupField.VALUE)
                .subAggregation(new SumAggregationBuilder("terms." + RollupField.COUNT_FIELD)
                        .field("longfield.terms." + RollupField.COUNT_FIELD));

        NumberFieldMapper.Builder nrBuilder = new NumberFieldMapper.Builder("terms", NumberFieldMapper.NumberType.LONG);
        MappedFieldType nrFTterm = nrBuilder.fieldType();
        nrFTterm.setHasDocValues(true);
        nrFTterm.setName(nonRollupTerms.field());

        NumberFieldMapper.Builder rBuilder = new NumberFieldMapper.Builder("terms", NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTterm = rBuilder.fieldType();
        rFTterm.setHasDocValues(true);
        rFTterm.setName(rollupTerms.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("terms." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("longfield.terms." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(longValueDoc(19L));
                    iw.addDocument(longValueDoc(19L));
                    iw.addDocument(longValueDoc(19L));
                }, nonRollupTerms,
                iw -> {
                    iw.addDocument(longValueRollupDoc(19L, 3));
                }, rollupTerms,
                new MappedFieldType[]{nrFTterm}, new MappedFieldType[]{rFTterm, rFTvalue});

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 0);
        assertThat(unrolled.toString(), equalTo(responses.get(0).toString()));
        assertThat(unrolled.toString(), not(equalTo(responses.get(1).toString())));
    }

    public void testHisto() throws IOException {
        HistogramAggregationBuilder nonRollupHisto = new HistogramAggregationBuilder("histo")
                .field("bar").interval(100);

        HistogramAggregationBuilder rollupHisto = new HistogramAggregationBuilder("histo")
                .field("bar.histogram." + RollupField.VALUE)
                .interval(100)
                .subAggregation(new SumAggregationBuilder("histo." + RollupField.COUNT_FIELD)
                        .field("bar.histogram." + RollupField.COUNT_FIELD));

        NumberFieldMapper.Builder nrBuilder = new NumberFieldMapper.Builder("histo", NumberFieldMapper.NumberType.LONG);
        MappedFieldType nrFTbar = nrBuilder.fieldType();
        nrFTbar.setHasDocValues(true);
        nrFTbar.setName(nonRollupHisto.field());

        NumberFieldMapper.Builder rBuilder = new NumberFieldMapper.Builder("histo", NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTbar = rBuilder.fieldType();
        rFTbar.setHasDocValues(true);
        rFTbar.setName(rollupHisto.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("histo." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("bar.histogram." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, nonRollupHisto,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 1));
                    iw.addDocument(timestampedValueRollupDoc(200, 2));
                    iw.addDocument(timestampedValueRollupDoc(300, 3));
                }, rollupHisto,
                new MappedFieldType[]{nrFTbar}, new MappedFieldType[]{rFTbar, rFTvalue});

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), null, null, 0);
        assertThat(unrolled.toString(), equalTo(responses.get(0).toString()));
        assertThat(unrolled.toString(), not(equalTo(responses.get(1).toString())));
    }

    public void testOverlappingBuckets() throws IOException {
        DateHistogramAggregationBuilder nonRollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp").fixedInterval(new DateHistogramInterval("100ms"));

        DateHistogramAggregationBuilder rollupHisto = new DateHistogramAggregationBuilder("histo")
                .field("timestamp.date_histogram." + RollupField.TIMESTAMP)
                .fixedInterval(new DateHistogramInterval("100ms"))
                .subAggregation(new SumAggregationBuilder("histo." + RollupField.COUNT_FIELD)
                        .field("timestamp.date_histogram." + RollupField.COUNT_FIELD));

        DateFieldMapper.Builder nrBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType nrFTtimestamp = nrBuilder.fieldType();
        nrFTtimestamp.setHasDocValues(true);
        nrFTtimestamp.setName(nonRollupHisto.field());

        DateFieldMapper.Builder rBuilder = new DateFieldMapper.Builder("histo");
        DateFieldMapper.DateFieldType rFTtimestamp = rBuilder.fieldType();
        rFTtimestamp.setHasDocValues(true);
        rFTtimestamp.setName(rollupHisto.field());

        NumberFieldMapper.Builder valueBuilder = new NumberFieldMapper.Builder("histo." + RollupField.COUNT_FIELD,
                NumberFieldMapper.NumberType.LONG);
        MappedFieldType rFTvalue = valueBuilder.fieldType();
        rFTvalue.setHasDocValues(true);
        rFTvalue.setName("timestamp.date_histogram." + RollupField.COUNT_FIELD);

        List<InternalAggregation> responses = doQueries(new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(timestampedValueDoc(100, 1));
                    iw.addDocument(timestampedValueDoc(200, 2));
                    iw.addDocument(timestampedValueDoc(300, 3));
                }, nonRollupHisto,
                iw -> {
                    iw.addDocument(timestampedValueRollupDoc(100, 100));
                    iw.addDocument(timestampedValueRollupDoc(200, 200));
                    iw.addDocument(timestampedValueRollupDoc(300, 300));
                    iw.addDocument(timestampedValueRollupDoc(400, 4)); // <-- Only one that should show up in rollup
                }, rollupHisto,
                new MappedFieldType[]{nrFTtimestamp}, new MappedFieldType[]{rFTtimestamp, rFTvalue});

        InternalAggregation unrolled = RollupResponseTranslator.unrollAgg(responses.get(1), responses.get(0), null, 0);
        assertThat(((InternalDateHistogram)unrolled).getBuckets().size(), equalTo(1));
        assertThat(((InternalDateHistogram)unrolled).getBuckets().get(0).getDocCount(), equalTo(1L));
        assertThat(((InternalDateHistogram)unrolled).getBuckets().get(0).getKeyAsString(), equalTo("1970-01-01T00:00:00.400Z"));
        assertThat(unrolled.toString(), equalTo("{\"histo\":{\"buckets\":[{\"key_as_string\":\"1970-01-01T00:00:00.400Z\"," +
                "\"key\":400,\"doc_count\":1}]}}"));
        assertThat(unrolled.toString(), not(equalTo(responses.get(1).toString())));
    }

    private Document timestampedValueDoc(long timestamp, long value) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("timestamp", timestamp));
        doc.add(new SortedNumericDocValuesField("foo", value));
        doc.add(new SortedNumericDocValuesField("bar", value));
        return doc;
    }

    private Document timestampedValueRollupDoc(long timestamp, long value) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.TIMESTAMP, timestamp));
        doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.COUNT_FIELD, 1));
        doc.add(new SortedNumericDocValuesField("timestamp.date_histogram." + RollupField.INTERVAL, 1));
        doc.add(new SortedNumericDocValuesField("foo.avg." + RollupField.VALUE, value));
        doc.add(new SortedNumericDocValuesField("foo.avg." + RollupField.COUNT_FIELD, 3));
        doc.add(new SortedNumericDocValuesField("foo.min." + RollupField.VALUE, value));
        doc.add(new SortedNumericDocValuesField("foo.max." + RollupField.VALUE, value));
        doc.add(new SortedNumericDocValuesField("foo.sum." + RollupField.VALUE, value));
        doc.add(new SortedNumericDocValuesField("bar.histogram." + RollupField.VALUE, value));
        doc.add(new SortedNumericDocValuesField("bar.histogram." + RollupField.COUNT_FIELD, 1));
        doc.add(new SortedNumericDocValuesField("bar.histogram." + RollupField.INTERVAL, 1));
        return doc;
    }

    private Document stringValueDoc(String stringValue) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("stringField", new BytesRef(stringValue)));
        return doc;
    }

    private Document stringValueRollupDoc(String stringValue, long docCount) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("stringfield.terms." + RollupField.VALUE, new BytesRef(stringValue)));
        doc.add(new SortedNumericDocValuesField("stringfield.terms." + RollupField.COUNT_FIELD, docCount));
        return doc;
    }

    private Document longValueDoc(Long longValue) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("longField", longValue));
        return doc;
    }

    private Document longValueRollupDoc(Long longValue, long docCount) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("longfield.terms." + RollupField.VALUE, longValue));
        doc.add(new SortedNumericDocValuesField("longfield.terms." + RollupField.COUNT_FIELD, docCount));
        return doc;
    }

    private List<InternalAggregation> doQueries(Query query,
                                               CheckedConsumer<RandomIndexWriter, IOException> buildNonRollupIndex,
                                               AggregationBuilder nonRollupAggBuilder,
                                               CheckedConsumer<RandomIndexWriter, IOException> buildRollupIndex,
                                               AggregationBuilder rollupAggBuilder,
                                               MappedFieldType[] nonRollupFieldType,
                                               MappedFieldType[] rollupFieldType)
            throws IOException {

        List<InternalAggregation> results = new ArrayList<>(2);
        results.add(doQuery(query, buildNonRollupIndex, nonRollupAggBuilder, nonRollupFieldType));
        results.add(doQuery(query, buildRollupIndex, rollupAggBuilder, rollupFieldType));

        return results;
    }

    private InternalAggregation doQuery(Query query,
                                    CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                                    AggregationBuilder aggBuilder, MappedFieldType[] fieldType)
            throws IOException {

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        Aggregator aggregator = createAggregator(aggBuilder, indexSearcher, fieldType);
        try {
            aggregator.preCollection();
            indexSearcher.search(query, aggregator);
            aggregator.postCollection();
            return aggregator.buildAggregation(0L);
        } finally {
            indexReader.close();
            directory.close();
        }
    }

}
