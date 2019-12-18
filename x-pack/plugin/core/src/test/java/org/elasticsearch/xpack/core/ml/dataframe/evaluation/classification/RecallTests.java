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
import java.util.List;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockCardinality;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockSingleValue;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTerms;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.TupleMatchers.isTuple;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class RecallTests extends AbstractSerializingTestCase<Recall> {

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
        Integer size = randomBoolean() ? null : randomIntBetween(1, 1000);
        return new Recall(size);
    }

    public void testConstructor_SizeValidationFailures() {
        {
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new Recall(-1));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
        {
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new Recall(0));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
        {
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new Recall(1001));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
    }

    public void testProcess() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME),
            mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123),
            mockCardinality(Recall.CARDINALITY_OF_ACTUAL_CLASS, 15),
            mockSingleValue("some_other_single_metric_agg", 0.2377)
        ));

        Recall recall = new Recall();
        recall.process(aggs);

        assertThat(recall.aggs("act", "pred"), isTuple(empty(), empty()));
        assertThat(recall.getResult().get(), equalTo(new Recall.Result(List.of(), 0.8123, 5)));
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
}
