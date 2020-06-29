/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.rollup.Rollup;
import org.hamcrest.core.IsEqual;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomHistogramGroupConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchActionTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(IndicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testNonZeroSize() {
        String[] normalIndices = new String[]{randomAlphaOfLength(10)};
        String[] rollupIndices = new String[]{randomAlphaOfLength(10)};
        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, Collections.emptySet());
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchAllQueryBuilder());
        source.size(100);
        source.aggregation(new DateHistogramAggregationBuilder("foo").field("foo").fixedInterval(new DateHistogramInterval("123ms")));
        SearchRequest request = new SearchRequest(normalIndices, source);
        NamedWriteableRegistry registry = mock(NamedWriteableRegistry.class);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.createMSearchRequest(request, registry, ctx));
        assertThat(e.getMessage(), equalTo("Rollup does not support returning search hits, please try again with [size: 0]."));
    }

    public void testBadQuery() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchPhraseQueryBuilder("foo", "bar"));
        source.aggregation(new DateHistogramAggregationBuilder("foo").field("foo").fixedInterval(new DateHistogramInterval("123ms")));
        source.size(0);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.rewriteQuery(new MatchPhraseQueryBuilder("foo", "bar"), Collections.emptySet()));
        assertThat(e.getMessage(), equalTo("Unsupported Query in search request: [match_phrase]"));
    }

    public void testRangeTimezoneUTC() {
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        QueryBuilder rewritten = TransportRollupSearchAction.rewriteQuery(new RangeQueryBuilder("foo").gt(1).timeZone("UTC"), caps);
        assertThat(rewritten, instanceOf(RangeQueryBuilder.class));
        assertThat(((RangeQueryBuilder)rewritten).fieldName(), equalTo("foo.date_histogram.timestamp"));
        assertThat(((RangeQueryBuilder)rewritten).timeZone(), equalTo("UTC"));
    }

    public void testRangeNullTimeZone() {
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h"), null, null));
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        QueryBuilder rewritten = TransportRollupSearchAction.rewriteQuery(new RangeQueryBuilder("foo").gt(1), caps);
        assertThat(rewritten, instanceOf(RangeQueryBuilder.class));
        assertThat(((RangeQueryBuilder)rewritten).fieldName(), equalTo("foo.date_histogram.timestamp"));
        assertNull(((RangeQueryBuilder)rewritten).timeZone());
    }

    public void testRangeDifferentTZ() {
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h"), null, "UTC"));
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        QueryBuilder rewritten = TransportRollupSearchAction.rewriteQuery(new RangeQueryBuilder("foo").gt(1).timeZone("CET"), caps);
        assertThat(rewritten, instanceOf(RangeQueryBuilder.class));
        assertThat(((RangeQueryBuilder)rewritten).fieldName(), equalTo("foo.date_histogram.timestamp"));
    }

    public void testTermQuery() {
        final TermsGroupConfig terms = new TermsGroupConfig("foo");
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("boo", new DateHistogramInterval("1h")), null, terms);
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        QueryBuilder rewritten = TransportRollupSearchAction.rewriteQuery(new TermQueryBuilder("foo", "bar"), caps);
        assertThat(rewritten, instanceOf(TermQueryBuilder.class));
        assertThat(((TermQueryBuilder)rewritten).fieldName(), equalTo("foo.terms.value"));
    }

    public void testTermsQuery() {
        final TermsGroupConfig terms = new TermsGroupConfig("foo");
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("boo", new DateHistogramInterval("1h")), null, terms);
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        QueryBuilder original = new TermsQueryBuilder("foo", Arrays.asList("bar", "baz"));
        QueryBuilder rewritten = TransportRollupSearchAction.rewriteQuery(original, caps);
        assertThat(rewritten, instanceOf(TermsQueryBuilder.class));
        assertNotSame(rewritten, original);
        assertThat(((TermsQueryBuilder)rewritten).fieldName(), equalTo("foo.terms.value"));
        assertThat(((TermsQueryBuilder)rewritten).values(),  equalTo(Arrays.asList("bar", "baz")));
    }

    public void testCompounds() {
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);

        BoolQueryBuilder builder = new BoolQueryBuilder();
        builder.must(getQueryBuilder(2));
        QueryBuilder rewritten = TransportRollupSearchAction.rewriteQuery(builder, caps);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        assertThat(((BoolQueryBuilder)rewritten).must().size(), equalTo(1));
    }

    public void testMatchAll() {
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        QueryBuilder rewritten = TransportRollupSearchAction.rewriteQuery(new MatchAllQueryBuilder(), caps);
        assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
    }

    public void testAmbiguousResolution() {
        final TermsGroupConfig terms = new TermsGroupConfig("foo");
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")), null, terms);
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.rewriteQuery(new RangeQueryBuilder("foo").gt(1), caps));
        assertThat(e.getMessage(), equalTo("Ambiguous field name resolution when mapping to rolled fields.  " +
                "Field name [foo] was mapped to: [foo.date_histogram.timestamp,foo.terms.value]."));
    }

    public static QueryBuilder getQueryBuilder(int levels) {
        if (levels == 0) {
            return ESTestCase.randomBoolean() ? new MatchAllQueryBuilder() : new RangeQueryBuilder("foo").gt(1);
        }

        int choice = ESTestCase.randomIntBetween(0,5);
        if (choice == 0) {
            BoolQueryBuilder b = new BoolQueryBuilder();
            b.must(getQueryBuilder(levels - 1));
            b.must(getQueryBuilder(levels - 1));
            b.mustNot(getQueryBuilder(levels - 1));
            b.should(getQueryBuilder(levels - 1));
            b.filter(getQueryBuilder(levels - 1));
            return b;
        } else if (choice == 1) {
            return new ConstantScoreQueryBuilder(getQueryBuilder(levels - 1));
        } else if (choice == 2) {
            return new BoostingQueryBuilder(getQueryBuilder(levels - 1), getQueryBuilder(levels - 1));
        } else if (choice == 3) {
            DisMaxQueryBuilder b = new DisMaxQueryBuilder();
            b.add(getQueryBuilder(levels - 1));
            b.add(getQueryBuilder(levels - 1));
        } else if (choice == 4) {
            return new MatchAllQueryBuilder();
        } else if (choice == 5) {
            return new TermQueryBuilder("foo", "bar");
        }

        return new RangeQueryBuilder("foo").gt(1);
    }

    public void testPostFilter() {
        String[] normalIndices = new String[]{randomAlphaOfLength(10)};
        String[] rollupIndices = new String[]{randomAlphaOfLength(10)};
        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, Collections.emptySet());
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(new DateHistogramAggregationBuilder("foo").field("foo").fixedInterval(new DateHistogramInterval("123ms")));
        source.postFilter(new TermQueryBuilder("foo", "bar"));
        source.size(0);
        SearchRequest request = new SearchRequest(normalIndices, source);
        NamedWriteableRegistry registry = mock(NamedWriteableRegistry.class);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.createMSearchRequest(request, registry, ctx));
        assertThat(e.getMessage(), equalTo("Rollup search does not support post filtering."));
    }

    public void testSuggest() {
        String[] normalIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.suggest(new SuggestBuilder());
        source.size(0);
        SearchRequest request = new SearchRequest(normalIndices, source);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.validateSearchRequest(request));
        assertThat(e.getMessage(), equalTo("Rollup search does not support suggestors."));
    }

    public void testHighlighters() {
        String[] normalIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.highlighter(new HighlightBuilder());
        source.size(0);
        SearchRequest request = new SearchRequest(normalIndices, source);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.validateSearchRequest(request));
        assertThat(e.getMessage(), equalTo("Rollup search does not support highlighting."));
    }

    public void testProfiling() {
        String[] normalIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.profile(true);
        source.size(0);
        SearchRequest request = new SearchRequest(normalIndices, source);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.validateSearchRequest(request));
        assertThat(e.getMessage(), equalTo("Rollup search does not support profiling at the moment."));
    }

    public void testExplain() {
        String[] normalIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.explain(true);
        source.size(0);
        SearchRequest request = new SearchRequest(normalIndices, source);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.validateSearchRequest(request));
        assertThat(e.getMessage(), equalTo("Rollup search does not support explaining."));
    }

    public void testNoRollupAgg() {
        String[] normalIndices = new String[]{};
        String[] rollupIndices = new String[]{randomAlphaOfLength(10)};
        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, Collections.emptySet());
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchAllQueryBuilder());
        source.size(0);
        SearchRequest request = new SearchRequest(rollupIndices, source);
        NamedWriteableRegistry registry = mock(NamedWriteableRegistry.class);
        MultiSearchRequest msearch = TransportRollupSearchAction.createMSearchRequest(request, registry, ctx);
        assertThat(msearch.requests().size(), equalTo(1));
        assertThat(msearch.requests().get(0), equalTo(request));
    }


    public void testNoLiveNoRollup() {
        String[] normalIndices = new String[0];
        String[] rollupIndices = new String[0];
        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, Collections.emptySet());
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchAllQueryBuilder());
        source.size(0);
        SearchRequest request = new SearchRequest(normalIndices, source);
        NamedWriteableRegistry registry = mock(NamedWriteableRegistry.class);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.createMSearchRequest(request, registry, ctx));
        assertThat(e.getMessage(), equalTo("Must specify at least one rollup index in _rollup_search API"));
    }

    public void testLiveOnlyCreateMSearch() {
        String[] normalIndices = new String[]{randomAlphaOfLength(10)};
        String[] rollupIndices = new String[0];
        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, Collections.emptySet());
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(new MatchAllQueryBuilder());
        source.size(0);
        SearchRequest request = new SearchRequest(normalIndices, source);
        NamedWriteableRegistry registry = mock(NamedWriteableRegistry.class);
        MultiSearchRequest msearch = TransportRollupSearchAction.createMSearchRequest(request, registry, ctx);
        assertThat(msearch.requests().size(), equalTo(1));
        assertThat(msearch.requests().get(0), equalTo(request));
    }

    public void testGood() {
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig config = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(config);
        Set<RollupJobCaps> caps = singleton(cap);

        String[] normalIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        String[] rollupIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        String[] combinedIndices = new String[]{normalIndices[0], rollupIndices[0]};

        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, caps);

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(getQueryBuilder(1));
        source.size(0);
        source.aggregation(new DateHistogramAggregationBuilder("foo").field("foo")
                .calendarInterval(config.getGroupConfig().getDateHistogram().getInterval()));
        SearchRequest request = new SearchRequest(combinedIndices, source);

        MultiSearchRequest msearch = TransportRollupSearchAction.createMSearchRequest(request, namedWriteableRegistry, ctx);
        assertThat(msearch.requests().size(), equalTo(2));
        assertThat(msearch.requests().get(0), equalTo(new SearchRequest(normalIndices, request.source())));

        SearchRequest normal = msearch.requests().get(0);
        assertThat(normal.indices().length, equalTo(1));
        assertThat(normal.indices()[0], equalTo(normalIndices[0]));

        SearchRequest rollup = msearch.requests().get(1);
        assertThat(rollup.indices().length, equalTo(1));
        assertThat(rollup.indices()[0], equalTo(rollupIndices[0]));
        assert(rollup.source().aggregations().getAggregatorFactories().iterator().next() instanceof FilterAggregationBuilder);
    }

    public void testGoodButNullQuery() {
        String[] normalIndices = new String[]{randomAlphaOfLength(10)};
        String[] rollupIndices = new String[]{randomAlphaOfLength(10)};
        String[] combinedIndices = new String[]{normalIndices[0], rollupIndices[0]};

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(null);
        source.size(0);
        source.aggregation(new DateHistogramAggregationBuilder("foo").field("foo").calendarInterval(new DateHistogramInterval("1d")));
        SearchRequest request = new SearchRequest(combinedIndices, source);

        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1d"), null, DateTimeZone.UTC.getID()));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        Set<RollupJobCaps> caps = singleton(new RollupJobCaps(job));

        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, caps);

        MultiSearchRequest msearch = TransportRollupSearchAction.createMSearchRequest(request, namedWriteableRegistry, ctx);
        assertThat(msearch.requests().size(), equalTo(2));
        assertThat(msearch.requests().get(0), equalTo(new SearchRequest(normalIndices, request.source())));

        SearchRequest normal = msearch.requests().get(0);
        assertThat(normal.indices().length, equalTo(1));
        assertThat(normal.indices()[0], equalTo(normalIndices[0]));

        SearchRequest rollup = msearch.requests().get(1);
        assertThat(rollup.indices().length, equalTo(1));
        assertThat(rollup.indices()[0], equalTo(rollupIndices[0]));
        assert(rollup.source().aggregations().getAggregatorFactories().iterator().next() instanceof FilterAggregationBuilder);
    }

    public void testTwoMatchingJobs() {
        final GroupConfig groupConfig = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")), null, null);
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        // so that the jobs aren't exactly equal
        final List<MetricConfig> metricConfigs = ConfigTestHelpers.randomMetricsConfigs(random());
        final RollupJobConfig job2 =
            new RollupJobConfig("foo2", "index", job.getRollupIndex(), "*/5 * * * * ?", 10,  groupConfig, metricConfigs, null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);

        String[] normalIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        String[] rollupIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        String[] combinedIndices = new String[]{normalIndices[0], rollupIndices[0]};

        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, caps);

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(getQueryBuilder(1));
        source.size(0);
        source.aggregation(new DateHistogramAggregationBuilder("foo").field("foo")
                .calendarInterval(job.getGroupConfig().getDateHistogram().getInterval()));
        SearchRequest request = new SearchRequest(combinedIndices, source);

        MultiSearchRequest msearch = TransportRollupSearchAction.createMSearchRequest(request, namedWriteableRegistry, ctx);
        assertThat(msearch.requests().size(), equalTo(2));

        assertThat(msearch.requests().get(0), equalTo(new SearchRequest(normalIndices, request.source())));
        SearchRequest normal = msearch.requests().get(0);
        assertThat(normal.indices().length, equalTo(1));
        assertThat(normal.indices()[0], equalTo(normalIndices[0]));

        SearchRequest rollup = msearch.requests().get(1);
        assertThat(rollup.indices().length, equalTo(1));
        assertThat(rollup.indices()[0], equalTo(rollupIndices[0]));
        assert(rollup.source().aggregations().getAggregatorFactories().iterator().next() instanceof FilterAggregationBuilder);

        assertThat(msearch.requests().size(), equalTo(2));
    }

    public void testTwoMatchingJobsOneBetter() {
        final GroupConfig groupConfig =
            new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")), null, null);
        final RollupJobConfig job =
            new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  groupConfig, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final GroupConfig groupConfig2 =
            new GroupConfig(groupConfig.getDateHistogram(), randomHistogramGroupConfig(random()), null);
        final RollupJobConfig job2 =
            new RollupJobConfig("foo2", "index", job.getRollupIndex(), "*/5 * * * * ?", 10,  groupConfig2, emptyList(), null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);

        String[] normalIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        String[] rollupIndices = new String[]{ESTestCase.randomAlphaOfLength(10)};
        String[] combinedIndices = new String[]{normalIndices[0], rollupIndices[0]};

        TransportRollupSearchAction.RollupSearchContext ctx
                = new TransportRollupSearchAction.RollupSearchContext(normalIndices, rollupIndices, caps);

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(getQueryBuilder(1));
        source.size(0);
        source.aggregation(new DateHistogramAggregationBuilder("foo").field("foo")
                .calendarInterval(job.getGroupConfig().getDateHistogram().getInterval()));
        SearchRequest request = new SearchRequest(combinedIndices, source);

        MultiSearchRequest msearch = TransportRollupSearchAction.createMSearchRequest(request, namedWriteableRegistry, ctx);
        assertThat(msearch.requests().size(), equalTo(2));

        assertThat(msearch.requests().get(0), equalTo(new SearchRequest(normalIndices, request.source())));
        SearchRequest normal = msearch.requests().get(0);
        assertThat(normal.indices().length, equalTo(1));
        assertThat(normal.indices()[0], equalTo(normalIndices[0]));

        SearchRequest rollup = msearch.requests().get(1);
        assertThat(rollup.indices().length, equalTo(1));
        assertThat(rollup.indices()[0], equalTo(rollupIndices[0]));
        assert(rollup.source().aggregations().getAggregatorFactories().iterator().next() instanceof FilterAggregationBuilder);


        // The executed query should match the first job ("foo") because the second job contained a histo and the first didn't,
        // so the first job will be "better"
        BoolQueryBuilder bool1 = new BoolQueryBuilder()
                .must(TransportRollupSearchAction.rewriteQuery(request.source().query(), caps))
                .filter(new TermQueryBuilder(RollupField.formatMetaField(RollupField.ID.getPreferredName()), "foo"))
                .filter(new TermsQueryBuilder(RollupField.formatMetaField(RollupField.VERSION_FIELD),
                    new long[]{Rollup.ROLLUP_VERSION_V1, Rollup.ROLLUP_VERSION_V2}));
        assertThat(msearch.requests().get(1).source().query(), equalTo(bool1));
    }

    public void testNoIndicesToSeparate() {
        String[] indices = new String[]{};
        ImmutableOpenMap<String, IndexMetadata> meta = ImmutableOpenMap.<String, IndexMetadata>builder().build();
        expectThrows(IllegalArgumentException.class, () -> TransportRollupSearchAction.separateIndices(indices, meta));
    }

    public void testSeparateAll() {
        String[] indices = new String[]{Metadata.ALL, "foo"};
        ImmutableOpenMap<String, IndexMetadata> meta = ImmutableOpenMap.<String, IndexMetadata>builder().build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.separateIndices(indices, meta));
        assertThat(e.getMessage(), equalTo("Searching _all via RollupSearch endpoint is not supported at this time."));
    }

    public void testEmptyMetadata() {
        String[] indices = new String[]{"foo", "bar"};
        ImmutableOpenMap<String, IndexMetadata> meta = ImmutableOpenMap.<String, IndexMetadata>builder().build();
        TransportRollupSearchAction.RollupSearchContext result
                = TransportRollupSearchAction.separateIndices(indices, meta);
        assertThat(result.getLiveIndices().length, equalTo(2));
        assertThat(result.getRollupIndices().length, equalTo(0));
        assertThat(result.getJobCaps().size(), equalTo(0));
    }

    public void testNoMatchingIndexInMetadata() {
        String[] indices = new String[]{"foo"};
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        ImmutableOpenMap.Builder<String, IndexMetadata> meta = ImmutableOpenMap.builder(1);
        meta.put("bar", indexMetadata);
        TransportRollupSearchAction.RollupSearchContext result
                = TransportRollupSearchAction.separateIndices(indices, meta.build());
        assertThat(result.getLiveIndices().length, equalTo(1));
        assertThat(result.getRollupIndices().length, equalTo(0));
        assertThat(result.getJobCaps().size(), equalTo(0));
    }

    public void testMatchingIndexInMetadata() throws IOException {
        String[] indices = new String[]{"foo"};

        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetadata mappingMeta = new MappingMetadata(RollupField.TYPE_NAME,
                Collections.singletonMap(RollupField.TYPE_NAME,
                        Collections.singletonMap("_meta",
                                Collections.singletonMap(RollupField.ROLLUP_META,
                                        Collections.singletonMap(jobName, job)))));

        IndexMetadata meta = Mockito.mock(IndexMetadata.class);
        when(meta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetadata> metaMap = ImmutableOpenMap.builder(1);
        metaMap.put("foo", meta);
        TransportRollupSearchAction.RollupSearchContext result
                = TransportRollupSearchAction.separateIndices(indices, metaMap.build());
        assertThat(result.getLiveIndices().length, equalTo(0));
        assertThat(result.getRollupIndices().length, equalTo(1));
        assertThat(result.getRollupIndices()[0], equalTo("foo"));
        assertThat(result.getJobCaps().size(), equalTo(1));
    }

    public void testLiveOnlyProcess() throws Exception {
        String[] indices = new String[]{"foo"};
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        ImmutableOpenMap.Builder<String, IndexMetadata> meta = ImmutableOpenMap.builder(1);
        meta.put("bar", indexMetadata);
        TransportRollupSearchAction.RollupSearchContext result
                = TransportRollupSearchAction.separateIndices(indices, meta.build());

        SearchResponse response = mock(SearchResponse.class);
        MultiSearchResponse.Item item = new MultiSearchResponse.Item(response, null);
        MultiSearchResponse msearchResponse = new MultiSearchResponse(new MultiSearchResponse.Item[]{item}, 1);

        SearchResponse r = TransportRollupSearchAction.processResponses(result,
                msearchResponse, mock(InternalAggregation.ReduceContext.class));
        assertThat(r, equalTo(response));
    }

    public void testRollupOnly() throws Exception {
        String[] indices = new String[]{"foo"};

        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetadata mappingMeta = new MappingMetadata(RollupField.TYPE_NAME,
                Collections.singletonMap(RollupField.TYPE_NAME,
                        Collections.singletonMap("_meta",
                                Collections.singletonMap(RollupField.ROLLUP_META,
                                        Collections.singletonMap(jobName, job)))));

        IndexMetadata indexMeta = Mockito.mock(IndexMetadata.class);
        when(indexMeta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetadata> metaMap = ImmutableOpenMap.builder(1);
        metaMap.put("foo", indexMeta);
        TransportRollupSearchAction.RollupSearchContext result
                = TransportRollupSearchAction.separateIndices(indices, metaMap.build());

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
        when(sum.getMetadata()).thenReturn(metadata);
        when(sum.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(sum);

        InternalSum count = mock(InternalSum.class);
        when(count.getValue()).thenReturn(2.0);
        when(count.value()).thenReturn(2.0);
        when(count.getName()).thenReturn("foo." + RollupField.COUNT_FIELD);
        when(count.getMetadata()).thenReturn(null);
        when(count.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(count);

        when(filter.getAggregations()).thenReturn(InternalAggregations.from(subaggs));
        when(filter.getName()).thenReturn("filter_foo");
        aggTree.add(filter);

        Aggregations mockAggs = InternalAggregations.from(aggTree);
        when(response.getAggregations()).thenReturn(mockAggs);
        MultiSearchResponse.Item item = new MultiSearchResponse.Item(response, null);
        MultiSearchResponse msearchResponse = new MultiSearchResponse(new MultiSearchResponse.Item[]{item}, 1);

        SearchResponse r = TransportRollupSearchAction.processResponses(result,
                msearchResponse, mock(InternalAggregation.ReduceContext.class));

        assertNotNull(r);
        Aggregations responseAggs = r.getAggregations();
        Avg avg = responseAggs.get("foo");
        assertThat(avg.getValue(), IsEqual.equalTo(5.0));
    }

    public void testTooManyRollups() throws IOException {
        String[] indices = new String[]{"foo", "bar"};

        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetadata mappingMeta = new MappingMetadata(RollupField.TYPE_NAME,
                Collections.singletonMap(RollupField.TYPE_NAME,
                        Collections.singletonMap("_meta",
                                Collections.singletonMap(RollupField.ROLLUP_META,
                                        Collections.singletonMap(jobName, job)))));

        IndexMetadata indexMeta = Mockito.mock(IndexMetadata.class);
        when(indexMeta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetadata> metaMap = ImmutableOpenMap.builder(2);
        metaMap.put("foo", indexMeta);
        metaMap.put("bar", indexMeta);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> TransportRollupSearchAction.separateIndices(indices, metaMap.build()));
        assertThat(e.getMessage(), equalTo("RollupSearch currently only supports searching one rollup index at a time. " +
            "Found the following rollup indices: [foo, bar]"));
    }

    public void testEmptyMsearch() {
        TransportRollupSearchAction.RollupSearchContext result
                = new TransportRollupSearchAction.RollupSearchContext(new String[0], new String[0], Collections.emptySet());
        MultiSearchResponse msearchResponse = new MultiSearchResponse(new MultiSearchResponse.Item[0], 1);

        RuntimeException e = expectThrows(RuntimeException.class, () -> TransportRollupSearchAction.processResponses(result,
                msearchResponse, mock(InternalAggregation.ReduceContext.class)));
        assertThat(e.getMessage(), equalTo("MSearch response was empty, cannot unroll RollupSearch results"));
    }

    public void testBoth() throws Exception {
        String[] indices = new String[]{"foo", "bar"};

        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetadata mappingMeta = new MappingMetadata(RollupField.TYPE_NAME,
                Collections.singletonMap(RollupField.TYPE_NAME,
                        Collections.singletonMap("_meta",
                                Collections.singletonMap(RollupField.ROLLUP_META,
                                        Collections.singletonMap(jobName, job)))));

        IndexMetadata indexMeta = Mockito.mock(IndexMetadata.class);
        when(indexMeta.mapping()).thenReturn(mappingMeta);

        MappingMetadata liveMappingMetadata = new MappingMetadata("bar", Collections.emptyMap());

        IndexMetadata liveIndexMeta = Mockito.mock(IndexMetadata.class);
        when(liveIndexMeta.mapping()).thenReturn(liveMappingMetadata);

        ImmutableOpenMap.Builder<String, IndexMetadata> metaMap = ImmutableOpenMap.builder(2);
        metaMap.put("foo", indexMeta);
        metaMap.put("bar", liveIndexMeta);
        TransportRollupSearchAction.RollupSearchContext separateIndices
                = TransportRollupSearchAction.separateIndices(indices, metaMap.build());


        SearchResponse protoResponse = mock(SearchResponse.class);
        when(protoResponse.getTook()).thenReturn(new TimeValue(100));
        List<InternalAggregation> protoAggTree = new ArrayList<>(1);
        InternalAvg internalAvg = new InternalAvg("foo", 10, 2, DocValueFormat.RAW, null);
        protoAggTree.add(internalAvg);
        Aggregations protoMockAggs = InternalAggregations.from(protoAggTree);
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
        when(sum.getMetadata()).thenReturn(metadata);
        when(sum.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(sum);

        InternalSum count = mock(InternalSum.class);
        when(count.getValue()).thenReturn(2.0);
        when(count.value()).thenReturn(2.0);
        when(count.getName()).thenReturn("foo." + RollupField.COUNT_FIELD);
        when(count.getMetadata()).thenReturn(null);
        when(count.getType()).thenReturn(SumAggregationBuilder.NAME);
        subaggs.add(count);

        when(filter.getAggregations()).thenReturn(InternalAggregations.from(subaggs));
        when(filter.getName()).thenReturn("filter_foo");
        aggTree.add(filter);

        Aggregations mockAggsWithout = InternalAggregations.from(aggTree);
        when(responseWithout.getAggregations()).thenReturn(mockAggsWithout);
        MultiSearchResponse.Item rolledResponse = new MultiSearchResponse.Item(responseWithout, null);

        MultiSearchResponse msearchResponse
                = new MultiSearchResponse(new MultiSearchResponse.Item[]{unrolledResponse, rolledResponse}, 123);

        SearchResponse response = TransportRollupSearchAction.processResponses(separateIndices, msearchResponse,
                mock(InternalAggregation.ReduceContext.class));

        assertNotNull(response);
        Aggregations responseAggs = response.getAggregations();
        assertNotNull(responseAggs);
        Avg avg = responseAggs.get("foo");
        assertThat(avg.getValue(), IsEqual.equalTo(5.0));

    }
}
