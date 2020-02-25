/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFilters;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockSingleValue;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTerms;
import static org.elasticsearch.test.hamcrest.TupleMatchers.isTuple;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class PrecisionTests extends AbstractSerializingTestCase<Precision> {

    @Override
    protected Precision doParseInstance(XContentParser parser) throws IOException {
        return Precision.fromXContent(parser);
    }

    @Override
    protected Precision createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Precision> instanceReader() {
        return Precision::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public static Precision createRandom() {
        return new Precision();
    }

    public void testProcess() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            mockTerms(Precision.ACTUAL_CLASSES_NAMES_AGG_NAME),
            mockFilters(Precision.BY_PREDICTED_CLASS_AGG_NAME),
            mockSingleValue(Precision.AVG_PRECISION_AGG_NAME, 0.8123),
            mockSingleValue("some_other_single_metric_agg", 0.2377)
        ));

        Precision precision = new Precision();
        precision.process(aggs);

        assertThat(precision.aggs("act", "pred"), isTuple(empty(), empty()));
        assertThat(precision.getResult().get(), equalTo(new Precision.Result(List.of(), 0.8123)));
    }

    public void testProcess_GivenMissingAgg() {
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                mockFilters(Precision.BY_PREDICTED_CLASS_AGG_NAME),
                mockSingleValue("some_other_single_metric_agg", 0.2377)
            ));
            Precision precision = new Precision();
            precision.process(aggs);
            assertThat(precision.getResult(), isEmpty());
        }
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                mockSingleValue(Precision.AVG_PRECISION_AGG_NAME, 0.8123),
                mockSingleValue("some_other_single_metric_agg", 0.2377)
            ));
            Precision precision = new Precision();
            precision.process(aggs);
            assertThat(precision.getResult(), isEmpty());
        }
    }

    public void testProcess_GivenAggOfWrongType() {
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                mockFilters(Precision.BY_PREDICTED_CLASS_AGG_NAME),
                mockFilters(Precision.AVG_PRECISION_AGG_NAME)
            ));
            Precision precision = new Precision();
            precision.process(aggs);
            assertThat(precision.getResult(), isEmpty());
        }
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                mockSingleValue(Precision.BY_PREDICTED_CLASS_AGG_NAME, 1.0),
                mockSingleValue(Precision.AVG_PRECISION_AGG_NAME, 0.8123)
            ));
            Precision precision = new Precision();
            precision.process(aggs);
            assertThat(precision.getResult(), isEmpty());
        }
    }

    public void testProcess_GivenCardinalityTooHigh() {
        Aggregations aggs =
            new Aggregations(Collections.singletonList(mockTerms(Precision.ACTUAL_CLASSES_NAMES_AGG_NAME, Collections.emptyList(), 1)));
        Precision precision = new Precision();
        precision.aggs("foo", "bar");
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> precision.process(aggs));
        assertThat(e.getMessage(), containsString("Cardinality of field [foo] is too high"));
    }
}
