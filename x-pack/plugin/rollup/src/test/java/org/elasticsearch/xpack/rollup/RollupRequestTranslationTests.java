/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;


import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.rollup.RollupRequestTranslator.translateAggregation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class RollupRequestTranslationTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testBasicDateHisto() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .extendedBounds(new ExtendedBounds(0L, 1000L))
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.dateHistogramInterval(), equalTo(new DateHistogramInterval("1d")));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));

        assertThat(filterConditions.size(), equalTo(1));
        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
                switch (((TermQueryBuilder) q).fieldName()) {
                    case "foo.date_histogram.time_zone":
                        assertThat(((TermQueryBuilder) q).value(), equalTo("UTC"));
                        break;
                    default:
                        fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                        break;
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }
    }

    public void testFormattedDateHisto() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
            .field("foo")
            .extendedBounds(new ExtendedBounds(0L, 1000L))
            .format("yyyy-MM-dd")
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.dateHistogramInterval(), equalTo(new DateHistogramInterval("1d")));
        assertThat(translatedHisto.format(), equalTo("yyyy-MM-dd"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
    }

    public void testSimpleMetric() {
        int i = ESTestCase.randomIntBetween(0, 2);
        List<AggregationBuilder> translated = new ArrayList<>();
        List<QueryBuilder> filterConditions = new ArrayList<>();

        Class clazz = null;
        String fieldName = null;
        int numAggs = 1;

        if (i == 0) {
            translated = translateAggregation(new MaxAggregationBuilder("test_metric")
                    .field("foo"), filterConditions, namedWriteableRegistry);
            clazz = MaxAggregationBuilder.class;
            fieldName =  "foo.max.value";
        } else if (i == 1) {
            translated = translateAggregation(new MinAggregationBuilder("test_metric")
                    .field("foo"), filterConditions, namedWriteableRegistry);
            clazz = MinAggregationBuilder.class;
            fieldName =  "foo.min.value";
        } else if (i == 2) {
            translated = translateAggregation(new SumAggregationBuilder("test_metric")
                    .field("foo"), filterConditions, namedWriteableRegistry);
            clazz = SumAggregationBuilder.class;
            fieldName =  "foo.sum.value";
        }

        assertThat(translated.size(), equalTo(numAggs));
        assertThat(translated.get(0), Matchers.instanceOf(clazz));
        assertThat((translated.get(0)).getName(), equalTo("test_metric"));
        assertThat(((ValuesSourceAggregationBuilder)translated.get(0)).field(), equalTo(fieldName));

        assertThat(filterConditions.size(), equalTo(0));
    }

    public void testUnsupportedMetric() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> translateAggregation(new StatsAggregationBuilder("test_metric")
                        .field("foo"), Collections.emptyList(), namedWriteableRegistry));
        assertThat(e.getMessage(), equalTo("Unable to translate aggregation tree into Rollup.  Aggregation [test_metric] is of type " +
                "[StatsAggregationBuilder] which is currently unsupported."));
    }

    public void testDateHistoIntervalWithMinMax() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.dateHistogramInterval().toString(), equalTo("1d"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));

        assertThat(filterConditions.size(), equalTo(1));

        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
               if (((TermQueryBuilder) q).fieldName().equals("foo.date_histogram.time_zone")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("UTC"));
                } else {
                    fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }
    }

    public void testDateHistoLongIntervalWithMinMax() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.interval(86400000)
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.interval(), equalTo(86400000L));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));

        assertThat(filterConditions.size(), equalTo(1));

        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
                if (((TermQueryBuilder) q).fieldName().equals("foo.date_histogram.time_zone")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("UTC"));
                }  else {
                    fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }
    }

    public void testAvgMetric() {
        List<QueryBuilder> filterConditions = new ArrayList<>();
        List<AggregationBuilder> translated = translateAggregation(new AvgAggregationBuilder("test_metric")
                .field("foo"), filterConditions, namedWriteableRegistry);

        assertThat(translated.size(), equalTo(2));
        Map<String, AggregationBuilder> metrics = translated.stream()
                .collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(metrics.get("test_metric.value"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)metrics.get("test_metric.value")).field(),
                equalTo("foo.avg.value"));

        assertThat(metrics.get("test_metric._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)metrics.get("test_metric._count")).field(),
                equalTo("foo.avg._count"));

        assertThat(filterConditions.size(), equalTo(0));
    }

    public void testStringTerms() throws IOException {

        TermsAggregationBuilder terms = new TermsAggregationBuilder("test_string_terms", ValueType.STRING);
        terms.field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        List<AggregationBuilder> translated = translateAggregation(terms, filterConditions, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(TermsAggregationBuilder.class));
        TermsAggregationBuilder translatedHisto = (TermsAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.field(), equalTo("foo.terms.value"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_string_terms._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_string_terms._count")).field(),
                equalTo("foo.terms._count"));

        assertThat(filterConditions.size(), equalTo(0));
    }

    public void testBasicHisto() {

        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.field("foo")
                .interval(1L)
                .extendedBounds(0.0, 1000.0)
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(HistogramAggregationBuilder.class));
        HistogramAggregationBuilder translatedHisto = (HistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.field(), equalTo("foo.histogram.value"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.histogram._count"));

        assertThat(filterConditions.size(), equalTo(0));
        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
                switch (((TermQueryBuilder) q).fieldName()) {
                    default:
                        fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                        break;
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }
    }

    public void testUnsupportedAgg() {
        GeoDistanceAggregationBuilder geo = new GeoDistanceAggregationBuilder("test_geo", new GeoPoint(0.0, 0.0));
        geo.field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        Exception e = expectThrows(RuntimeException.class,
                () -> translateAggregation(geo, filterConditions, namedWriteableRegistry));
        assertThat(e.getMessage(), equalTo("Unable to translate aggregation tree into Rollup.  Aggregation [test_geo] is of type " +
                "[GeoDistanceAggregationBuilder] which is currently unsupported."));
    }
}
