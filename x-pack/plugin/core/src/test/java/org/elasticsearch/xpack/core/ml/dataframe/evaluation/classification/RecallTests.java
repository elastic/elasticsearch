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
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.TupleMatchers.isTuple;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockSingleValue;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTerms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class RecallTests extends AbstractSerializingTestCase<Recall> {

    private static final EvaluationParameters EVALUATION_PARAMETERS = new EvaluationParameters(100);

    @Override
    protected Recall doParseInstance(XContentParser parser) throws IOException {
        return Recall.fromXContent(parser);
    }

    @Override
    protected Recall createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Recall> instanceReader() {
        return Recall::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public static Recall createRandom() {
        return new Recall();
    }

    public void testProcess() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME),
            mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123),
            mockSingleValue("some_other_single_metric_agg", 0.2377)
        ));

        Recall recall = new Recall();
        recall.process(aggs);

        assertThat(recall.aggs(EVALUATION_PARAMETERS, "act", "pred"), isTuple(empty(), empty()));
        assertThat(recall.getResult().get(), equalTo(new Recall.Result(List.of(), 0.8123)));
    }

    public void testProcess_GivenMissingAgg() {
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME),
                mockSingleValue("some_other_single_metric_agg", 0.2377)
            ));
            Recall recall = new Recall();
            recall.process(aggs);
            assertThat(recall.getResult(), isEmpty());
        }
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123),
                mockSingleValue("some_other_single_metric_agg", 0.2377)
            ));
            Recall recall = new Recall();
            recall.process(aggs);
            assertThat(recall.getResult(), isEmpty());
        }
    }

    public void testProcess_GivenAggOfWrongType() {
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME),
                mockTerms(Recall.AVG_RECALL_AGG_NAME)
            ));
            Recall recall = new Recall();
            recall.process(aggs);
            assertThat(recall.getResult(), isEmpty());
        }
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                mockSingleValue(Recall.BY_ACTUAL_CLASS_AGG_NAME, 1.0),
                mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123)
            ));
            Recall recall = new Recall();
            recall.process(aggs);
            assertThat(recall.getResult(), isEmpty());
        }
    }

    public void testProcess_GivenCardinalityTooHigh() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME, Collections.emptyList(), 1),
            mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123)));
        Recall recall = new Recall();
        recall.aggs(EVALUATION_PARAMETERS, "foo", "bar");
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> recall.process(aggs));
        assertThat(e.getMessage(), containsString("Cardinality of field [foo] is too high"));
    }
}
