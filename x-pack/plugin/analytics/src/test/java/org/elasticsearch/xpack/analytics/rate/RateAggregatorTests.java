/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.analytics.AnalyticsTestsUtils.histogramFieldDocValues;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class RateAggregatorTests extends AggregatorTestCase {

    /**
     * Script to return the {@code _value} provided by aggs framework.
     */
    public static final String ADD_ONE_SCRIPT = "add_one";

    public static final String TERM_FILTERING = "term_filtering";

    public static final String DATE_FIELD = "t";

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(ADD_ONE_SCRIPT, vars -> {
            LeafDocLookup leafDocLookup = (LeafDocLookup) vars.get("doc");
            String fieldname = (String) vars.get("fieldname");
            ScriptDocValues<?> scriptDocValues = leafDocLookup.get(fieldname);
            return ((Number) scriptDocValues.get(0)).doubleValue() + 1.0;
        });

        scripts.put(TERM_FILTERING, vars -> {
            LeafDocLookup leafDocLookup = (LeafDocLookup) vars.get("doc");
            int term = (Integer) vars.get("term");
            ScriptDocValues<?> termDocValues = leafDocLookup.get("term");
            int currentTerm = ((Number) termDocValues.get(0)).intValue();
            if (currentTerm == term) {
                return ((Number) leafDocLookup.get("field").get(0)).doubleValue();
            }
            return null;
        });

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), "month", true, "month", "val", iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("wrong_val", 102)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("wrong_val", 103)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("wrong_val", 103)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(dh.getBuckets().get(0).getAggregations().asList(), hasSize(1));
            assertThat(dh.getBuckets().get(0).getAggregations().asList().get(0), instanceOf(InternalRate.class));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(0.0, 0.000001));

            assertThat(dh.getBuckets().get(1).getAggregations().asList(), hasSize(1));
            assertThat(dh.getBuckets().get(1).getAggregations().asList().get(0), instanceOf(InternalRate.class));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(0.0, 0.000001));
        });
    }

    public void testSortedNumericDocValuesMonthToMonth() throws IOException {
        testCase(new MatchAllDocsQuery(), "month", true, "month", "val", iw -> {
            iw.addDocument(
                doc("2010-03-12T01:07:45", new SortedNumericDocValuesField("val", 1), new SortedNumericDocValuesField("val", 2))
            );
            iw.addDocument(doc("2010-04-01T03:43:34", new SortedNumericDocValuesField("val", 3)));
            iw.addDocument(
                doc("2010-04-27T03:43:34", new SortedNumericDocValuesField("val", 4), new SortedNumericDocValuesField("val", 5))
            );
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(12.0, 0.000001));
        });
    }

    public void testDocValuesMonthToMonth() throws IOException {
        testCase(new MatchAllDocsQuery(), "month", true, "month", "val", iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 4)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(1.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(7.0, 0.000001));
        });
    }

    public void testDocValuesMonthToMonthValueCount() throws IOException {
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month")
            .field("val")
            .rateMode("value_count");

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date");
        dateHistogramAggregationBuilder.field(DATE_FIELD);
        dateHistogramAggregationBuilder.calendarInterval(new DateHistogramInterval("month"));

        dateHistogramAggregationBuilder.subAggregation(rateAggregationBuilder);
        testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", new SortedNumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-04-01T03:43:34", new SortedNumericDocValuesField("val", 3)));
            iw.addDocument(
                doc("2010-04-27T03:43:34", new SortedNumericDocValuesField("val", 4), new SortedNumericDocValuesField("val", 5))
            );
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(1.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));
        }, dateType, numType);
    }

    public void testDocValuesMonthToMonthDefaultRate() throws IOException {
        testCase(new MatchAllDocsQuery(), "month", true, null, "val", iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 4)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(1.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(7.0, 0.000001));
        });
    }

    public void testDocValuesYearToMonth() throws IOException {
        testCase(new MatchAllDocsQuery(), "year", true, "month", "val", iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 8)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(1));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(1.0, 0.000001));
        });
    }

    public void testDocValuesMonthToYear() throws IOException {
        testCase(new MatchAllDocsQuery(), "month", true, "year", "val", iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 8)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(12.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(132.0, 0.000001));
        });
    }

    public void testDocValues50DaysToDays() throws IOException {
        testCase(new MatchAllDocsQuery(), "50d", false, "day", "val", iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 8)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(0.02, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(0.22, 0.000001));
        });
    }

    public void testIncompatibleCalendarRate() {
        String interval = randomFrom("second", "minute", "hour", "day", "week", "1s", "1m", "1h", "1d", "1w");
        String rate = randomFrom("month", "quarter", "year", "1M", "1q", "1y");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(new MatchAllDocsQuery(), interval, true, rate, "val", iw -> {
                iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
                iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
                iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 8)));
            }, dh -> { fail("Shouldn't be here"); })
        );
        assertEquals(
            "Cannot use month-based rate unit ["
                + RateAggregationBuilder.parse(rate).shortName()
                + "] with non-month based calendar interval histogram ["
                + RateAggregationBuilder.parse(interval).shortName()
                + "] only week, day, hour, minute and second are supported for this histogram",
            ex.getMessage()
        );
    }

    public void testIncompatibleIntervalRate() {
        String interval = randomFrom("1s", "2m", "4h", "5d");
        String rate = randomFrom("month", "quarter", "year", "1M", "1q", "1y");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(new MatchAllDocsQuery(), interval, false, rate, "val", iw -> {
                iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
                iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
                iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 8)));
            }, dh -> { fail("Shouldn't be here"); })
        );
        assertEquals(
            "Cannot use month-based rate unit ["
                + RateAggregationBuilder.parse(rate).shortName()
                + "] with fixed interval based histogram, only week, day, hour, minute and second are supported for this histogram",
            ex.getMessage()
        );
    }

    public void testNoFieldMonthToDay() throws IOException {
        testCase(new MatchAllDocsQuery(), "month", true, "day", null, iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 4)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(1 / 31.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(2 / 30.0, 0.000001));
        });
    }

    public void testNoWrapping() throws IOException {
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("day");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(rateAggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
                iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
                iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 4)));
            }, h -> { fail("Shouldn't be here"); }, dateType, numType)
        );
        assertEquals("The rate aggregation can only be used inside a date histogram", ex.getMessage());
    }

    public void testDoubleWrapping() throws IOException {
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month").field("val");
        if (randomBoolean()) {
            rateAggregationBuilder.rateMode("sum");
        }
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("month"))
            .subAggregation(rateAggregationBuilder);
        DateHistogramAggregationBuilder topDateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date");
        topDateHistogramAggregationBuilder.field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("year"))
            .subAggregation(dateHistogramAggregationBuilder);

        testCase(topDateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(doc("2009-03-12T01:07:45", new NumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 2)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 4)));
        }, (Consumer<InternalDateHistogram>) tdh -> {
            assertThat(tdh.getBuckets(), hasSize(2));
            InternalDateHistogram dh1 = (InternalDateHistogram) tdh.getBuckets().get(0).getAggregations().asList().get(0);
            assertThat(dh1.getBuckets(), hasSize(1));
            assertThat(((InternalRate) dh1.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(1.0, 0.000001));

            InternalDateHistogram dh2 = (InternalDateHistogram) tdh.getBuckets().get(1).getAggregations().asList().get(0);
            assertThat(dh2.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh2.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(2.0, 0.000001));
            assertThat(((InternalRate) dh2.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(7.0, 0.000001));
        }, dateType, numType);
    }

    public void testKeywordSandwich() throws IOException {
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType keywordType = new KeywordFieldMapper.KeywordFieldType("term");
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month").field("val");
        if (randomBoolean()) {
            rateAggregationBuilder.rateMode("sum");
        }
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("my_term").field("term")
            .subAggregation(rateAggregationBuilder);
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("month"))
            .subAggregation(termsAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(
                doc(
                    "2010-03-11T01:07:45",
                    new NumericDocValuesField("val", 1),
                    new IntPoint("val", 1),
                    new SortedSetDocValuesField("term", new BytesRef("a"))
                )
            );
            iw.addDocument(
                doc(
                    "2010-03-12T01:07:45",
                    new NumericDocValuesField("val", 2),
                    new IntPoint("val", 2),
                    new SortedSetDocValuesField("term", new BytesRef("a"))
                )
            );
            iw.addDocument(
                doc(
                    "2010-04-01T03:43:34",
                    new NumericDocValuesField("val", 3),
                    new IntPoint("val", 3),
                    new SortedSetDocValuesField("term", new BytesRef("a"))
                )
            );
            iw.addDocument(
                doc(
                    "2010-04-27T03:43:34",
                    new NumericDocValuesField("val", 4),
                    new IntPoint("val", 4),
                    new SortedSetDocValuesField("term", new BytesRef("b"))
                )
            );
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            StringTerms st1 = (StringTerms) dh.getBuckets().get(0).getAggregations().asList().get(0);
            assertThat(st1.getBuckets(), hasSize(1));
            assertThat(((InternalRate) st1.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));

            StringTerms st2 = (StringTerms) dh.getBuckets().get(1).getAggregations().asList().get(0);
            assertThat(st2.getBuckets(), hasSize(2));
            assertThat(((InternalRate) st2.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));
            assertThat(((InternalRate) st2.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(4.0, 0.000001));
        }, dateType, numType, keywordType);
    }

    public void testUnsupportedKeywordSandwich() throws IOException {
        String rate;
        String histogram;
        boolean millisecondBasedRate = randomBoolean();
        if (millisecondBasedRate) {
            rate = randomFrom("second", "minute", "day", "week");
            histogram = randomFrom("month", "quarter", "year");
        } else {
            rate = randomFrom("month", "quarter", "year");
            histogram = randomFrom("second", "minute", "day", "week");
        }

        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType keywordType = new KeywordFieldMapper.KeywordFieldType("term");
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit(rate).field("val");
        if (randomBoolean()) {
            rateAggregationBuilder.rateMode("sum");
        }
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("my_term").field("term")
            .subAggregation(rateAggregationBuilder);
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval(histogram))
            .subAggregation(termsAggregationBuilder);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class, () -> testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(
                doc(
                    "2010-03-11T01:07:45",
                    new NumericDocValuesField("val", 1),
                    new IntPoint("val", 1),
                    new SortedSetDocValuesField("term", new BytesRef("a"))
                )
            );
            iw.addDocument(
                doc(
                    "2010-03-12T01:07:45",
                    new NumericDocValuesField("val", 2),
                    new IntPoint("val", 2),
                    new SortedSetDocValuesField("term", new BytesRef("a"))
                )
            );
            iw.addDocument(
                doc(
                    "2010-04-01T03:43:34",
                    new NumericDocValuesField("val", 3),
                    new IntPoint("val", 3),
                    new SortedSetDocValuesField("term", new BytesRef("a"))
                )
            );
            iw.addDocument(
                doc(
                    "2010-04-27T03:43:34",
                    new NumericDocValuesField("val", 4),
                    new IntPoint("val", 4),
                    new SortedSetDocValuesField("term", new BytesRef("b"))
                )
            );
        }, (Consumer<InternalDateHistogram>) dh -> {
            fail("Shouldn't be here");
        }, dateType, numType, keywordType));
        if (millisecondBasedRate) {
            assertEquals("Cannot use non month-based rate unit [" + rate + "] with calendar interval histogram [" +
                histogram + "] only month, quarter and year are supported for this histogram", ex.getMessage());
        } else {
            assertEquals("Cannot use month-based rate unit [" + rate + "] with non-month based calendar interval histogram [" +
                histogram + "] only week, day, hour, minute and second are supported for this histogram", ex.getMessage());
        }
    }

    public void testKeywordSandwichWithSorting() throws IOException {
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType keywordType = new KeywordFieldMapper.KeywordFieldType("term");
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("week").field("val");
        boolean useSum = randomBoolean();
        if (useSum) {
            if (randomBoolean()) {
                rateAggregationBuilder.rateMode("sum");
            }
        } else {
            rateAggregationBuilder.rateMode("value_count");
        }
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("my_term").field("term")
            .order(BucketOrder.aggregation("my_rate", false))
            .subAggregation(rateAggregationBuilder);
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("week"))
            .subAggregation(termsAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(
                doc("2020-11-02T01:07:45", new NumericDocValuesField("val", 1), new SortedSetDocValuesField("term", new BytesRef("a")))
            );
            iw.addDocument(
                doc("2020-11-03T01:07:45", new NumericDocValuesField("val", 2), new SortedSetDocValuesField("term", new BytesRef("a")))
            );
            iw.addDocument(
                doc("2020-11-04T03:43:34", new NumericDocValuesField("val", 4), new SortedSetDocValuesField("term", new BytesRef("b")))
            );
            iw.addDocument(
                doc("2020-11-09T03:43:34", new NumericDocValuesField("val", 30), new SortedSetDocValuesField("term", new BytesRef("a")))
            );
            iw.addDocument(
                doc("2020-11-10T03:43:34", new NumericDocValuesField("val", 4), new SortedSetDocValuesField("term", new BytesRef("b")))
            );
            iw.addDocument(
                doc("2020-11-11T03:43:34", new NumericDocValuesField("val", 4), new SortedSetDocValuesField("term", new BytesRef("b")))
            );
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            if (useSum) {
                StringTerms st1 = (StringTerms) dh.getBuckets().get(0).getAggregations().asList().get(0);
                assertThat(st1.getBuckets(), hasSize(2));
                assertThat(st1.getBuckets().get(0).getKeyAsString(), equalTo("b"));
                assertThat(((InternalRate) st1.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(4.0, 0.000001));
                assertThat(st1.getBuckets().get(1).getKeyAsString(), equalTo("a"));
                assertThat(((InternalRate) st1.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));

                StringTerms st2 = (StringTerms) dh.getBuckets().get(1).getAggregations().asList().get(0);
                assertThat(st2.getBuckets(), hasSize(2));
                assertThat(st2.getBuckets().get(0).getKeyAsString(), equalTo("a"));
                assertThat(((InternalRate) st2.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(30.0, 0.000001));
                assertThat(st2.getBuckets().get(1).getKeyAsString(), equalTo("b"));
                assertThat(((InternalRate) st2.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(8.0, 0.000001));
            } else {
                StringTerms st1 = (StringTerms) dh.getBuckets().get(0).getAggregations().asList().get(0);
                assertThat(st1.getBuckets(), hasSize(2));
                assertThat(st1.getBuckets().get(0).getKeyAsString(), equalTo("a"));
                assertThat(((InternalRate) st1.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(2.0, 0.000001));
                assertThat(st1.getBuckets().get(1).getKeyAsString(), equalTo("b"));
                assertThat(((InternalRate) st1.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(1.0, 0.000001));

                StringTerms st2 = (StringTerms) dh.getBuckets().get(1).getAggregations().asList().get(0);
                assertThat(st2.getBuckets(), hasSize(2));
                assertThat(st2.getBuckets().get(0).getKeyAsString(), equalTo("b"));
                assertThat(((InternalRate) st2.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(2.0, 0.000001));
                assertThat(st2.getBuckets().get(1).getKeyAsString(), equalTo("a"));
                assertThat(((InternalRate) st2.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(1.0, 0.000001));

            }
        }, dateType, numType, keywordType);
    }

    public void testScriptMonthToDay() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            "month",
            true,
            "day",
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, ADD_ONE_SCRIPT, Collections.singletonMap("fieldname", "val")),
            iw -> {
                iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 1)));
                iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
                iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 4)));
            },
            dh -> {
                assertThat(dh.getBuckets(), hasSize(2));
                assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(2 / 31.0, 0.000001));
                assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(9 / 30.0, 0.000001));
            }
        );
    }

    public void testFilter() throws IOException {
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType keywordType = new KeywordFieldMapper.KeywordFieldType("term");
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month").field("val");
        if (randomBoolean()) {
            rateAggregationBuilder.rateMode("sum");
        }

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("month"))
            .subAggregation(rateAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new TermQuery(new Term("term", "a")), iw -> {
            iw.addDocument(doc("2010-03-11T01:07:45", new NumericDocValuesField("val", 1), new StringField("term", "a", Field.Store.NO)));
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 2), new StringField("term", "a", Field.Store.NO)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3), new StringField("term", "a", Field.Store.NO)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 4), new StringField("term", "b", Field.Store.NO)));
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));
        }, dateType, numType, keywordType);
    }

    public void testFormatter() throws IOException {
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month")
            .field("val")
            .format("00.0/M");
        if (randomBoolean()) {
            rateAggregationBuilder.rateMode("sum");
        }

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("month"))
            .subAggregation(rateAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(doc("2010-03-11T01:07:45", new NumericDocValuesField("val", 1)));
            iw.addDocument(doc("2010-03-12T01:07:45", new NumericDocValuesField("val", 2)));
            iw.addDocument(doc("2010-04-01T03:43:34", new NumericDocValuesField("val", 3)));
            iw.addDocument(doc("2010-04-27T03:43:34", new NumericDocValuesField("val", 4)));
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).getValueAsString(), equalTo("03.0/M"));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).getValueAsString(), equalTo("07.0/M"));
        }, dateType, numType);
    }

    public void testHistogramFieldMonthToMonth() throws IOException {
        MappedFieldType histType = new HistogramFieldMapper.HistogramFieldType("val", Collections.emptyMap());
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month").field("val");
        if (randomBoolean()) {
            rateAggregationBuilder.rateMode("sum");
        }

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("month"))
            .subAggregation(rateAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(doc("2010-03-01T00:00:00", histogramFieldDocValues("val", new double[] { 1, 2 })));
            iw.addDocument(doc("2010-04-01T00:00:00", histogramFieldDocValues("val", new double[] { 3, 4 })));
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).getValue(), closeTo(3.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).getValue(), closeTo(7.0, 0.000001));
        }, dateType, histType);
    }

    public void testHistogramFieldMonthToYear() throws IOException {
        MappedFieldType histType = new HistogramFieldMapper.HistogramFieldType("val", Collections.emptyMap());
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month").field("val");
        if (randomBoolean()) {
            rateAggregationBuilder.rateMode("sum");
        }

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("year"))
            .subAggregation(rateAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(doc("2010-03-01T00:00:00", histogramFieldDocValues("val", new double[] { 1, 2 })));
            iw.addDocument(doc("2010-04-01T00:00:00", histogramFieldDocValues("val", new double[] { 3, 4 })));
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(1));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).getValue(), closeTo(10.0 / 12, 0.000001));
        }, dateType, histType);
    }

    public void testHistogramFieldMonthToMonthValueCount() throws IOException {
        MappedFieldType histType = new HistogramFieldMapper.HistogramFieldType("val", Collections.emptyMap());
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month")
            .rateMode("value_count")
            .field("val");

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("month"))
            .subAggregation(rateAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(doc("2010-03-01T00:00:00", histogramFieldDocValues("val", new double[] { 1, 2 })));
            iw.addDocument(doc("2010-04-01T00:00:00", histogramFieldDocValues("val", new double[] { 3, 4, 5 })));
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).getValue(), closeTo(2.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).getValue(), closeTo(3.0, 0.000001));
        }, dateType, histType);
    }

    public void testHistogramFieldMonthToYearValueCount() throws IOException {
        MappedFieldType histType = new HistogramFieldMapper.HistogramFieldType("val", Collections.emptyMap());
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month")
            .rateMode("value_count")
            .field("val");

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("year"))
            .subAggregation(rateAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(doc("2010-03-01T00:00:00", histogramFieldDocValues("val", new double[] { 1, 2 })));
            iw.addDocument(doc("2010-04-01T00:00:00", histogramFieldDocValues("val", new double[] { 3, 4, 5 })));
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(1));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).getValue(), closeTo(5.0 / 12, 0.000001));
        }, dateType, histType);
    }

    public void testFilterWithHistogramField() throws IOException {
        MappedFieldType histType = new HistogramFieldMapper.HistogramFieldType("val", Collections.emptyMap());
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType keywordType = new KeywordFieldMapper.KeywordFieldType("term");
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month").field("val");

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("month"))
            .subAggregation(rateAggregationBuilder);

        testCase(dateHistogramAggregationBuilder, new TermQuery(new Term("term", "a")), iw -> {
            iw.addDocument(doc("2010-03-01T00:00:00", histogramFieldDocValues("val", new double[] { 1, 2 }),
                new StringField("term", "a", Field.Store.NO)));
            iw.addDocument(doc("2010-04-01T00:00:00", histogramFieldDocValues("val", new double[] { 3 }),
                new StringField("term", "a", Field.Store.NO)));
            iw.addDocument(doc("2010-04-01T00:00:00", histogramFieldDocValues("val", new double[] { 4 }),
                new StringField("term", "b", Field.Store.NO)));
        }, (Consumer<InternalDateHistogram>) dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(((InternalRate) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));
            assertThat(((InternalRate) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(3.0, 0.000001));
        }, dateType, histType, keywordType);
    }

    public void testModeWithoutField() {
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate").rateUnit("month").rateMode("sum");

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date").field(DATE_FIELD)
            .calendarInterval(new DateHistogramInterval("month"))
            .subAggregation(rateAggregationBuilder);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(dateHistogramAggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(doc("2010-03-12T01:07:45", new SortedNumericDocValuesField("val", 1)));
            }, h -> { fail("Shouldn't be here"); }, dateType, numType)
        );
        assertEquals("The mode parameter is only supported with field or script", ex.getMessage());
    }

    private void testCase(
        Query query,
        String interval,
        boolean isCalendar,
        String unit,
        Object field,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalDateHistogram> verify
    ) throws IOException {
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        RateAggregationBuilder rateAggregationBuilder = new RateAggregationBuilder("my_rate");
        if (unit != null) {
            rateAggregationBuilder.rateUnit(unit);
        }
        if (field != null) {
            if (field instanceof Script) {
                rateAggregationBuilder.script((Script) field);
            } else {
                rateAggregationBuilder.field((String) field);
            }
        }
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder("my_date");
        dateHistogramAggregationBuilder.field(DATE_FIELD);
        if (isCalendar) {
            dateHistogramAggregationBuilder.calendarInterval(new DateHistogramInterval(interval));
        } else {
            dateHistogramAggregationBuilder.fixedInterval(new DateHistogramInterval(interval));
        }
        dateHistogramAggregationBuilder.subAggregation(rateAggregationBuilder);
        testCase(dateHistogramAggregationBuilder, query, buildIndex, verify, dateType, numType);
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new AnalyticsPlugin());
    }

    private DateFieldMapper.DateFieldType dateFieldType(String name) {
        return new DateFieldMapper.DateFieldType(
            name,
            true,
            false,
            true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
    }

    private Iterable<IndexableField> doc(String date, IndexableField... fields) {
        List<IndexableField> indexableFields = new ArrayList<>();
        long instant = dateFieldType(DATE_FIELD).parse(date);
        indexableFields.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
        indexableFields.add(new LongPoint(DATE_FIELD, instant));
        indexableFields.addAll(Arrays.asList(fields));
        return indexableFields;
    }
}
