/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
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

public class RecallTests extends AbstractXContentSerializingTestCase<Recall> {

    private static final EvaluationParameters EVALUATION_PARAMETERS = new EvaluationParameters(100);
    private static final EvaluationFields EVALUATION_FIELDS = new EvaluationFields("foo", "bar", null, null, null, true);

    @Override
    protected Recall doParseInstance(XContentParser parser) throws IOException {
        return Recall.fromXContent(parser);
    }

    @Override
    protected Recall createTestInstance() {
        return createRandom();
    }

    @Override
    protected Recall mutateInstance(Recall instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
        InternalAggregations aggs = InternalAggregations.from(
            Arrays.asList(
                mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME),
                mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123),
                mockSingleValue("some_other_single_metric_agg", 0.2377)
            )
        );

        Recall recall = new Recall();
        recall.process(aggs);

        assertThat(recall.aggs(EVALUATION_PARAMETERS, EVALUATION_FIELDS), isTuple(empty(), empty()));
        assertThat(recall.getResult().get(), equalTo(new Recall.Result(List.of(), 0.8123)));
    }

    public void testProcess_GivenMissingAgg() {
        {
            InternalAggregations aggs = InternalAggregations.from(
                Arrays.asList(mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME), mockSingleValue("some_other_single_metric_agg", 0.2377))
            );
            Recall recall = new Recall();
            recall.process(aggs);
            assertThat(recall.getResult(), isEmpty());
        }
        {
            InternalAggregations aggs = InternalAggregations.from(
                Arrays.asList(mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123), mockSingleValue("some_other_single_metric_agg", 0.2377))
            );
            Recall recall = new Recall();
            recall.process(aggs);
            assertThat(recall.getResult(), isEmpty());
        }
    }

    public void testProcess_GivenAggOfWrongType() {
        {
            InternalAggregations aggs = InternalAggregations.from(
                Arrays.asList(mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME), mockTerms(Recall.AVG_RECALL_AGG_NAME))
            );
            Recall recall = new Recall();
            recall.process(aggs);
            assertThat(recall.getResult(), isEmpty());
        }
        {
            InternalAggregations aggs = InternalAggregations.from(
                Arrays.asList(mockSingleValue(Recall.BY_ACTUAL_CLASS_AGG_NAME, 1.0), mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123))
            );
            Recall recall = new Recall();
            recall.process(aggs);
            assertThat(recall.getResult(), isEmpty());
        }
    }

    public void testProcess_GivenCardinalityTooHigh() {
        InternalAggregations aggs = InternalAggregations.from(
            Arrays.asList(
                mockTerms(Recall.BY_ACTUAL_CLASS_AGG_NAME, Collections.emptyList(), 1),
                mockSingleValue(Recall.AVG_RECALL_AGG_NAME, 0.8123)
            )
        );
        Recall recall = new Recall();
        recall.aggs(EVALUATION_PARAMETERS, EVALUATION_FIELDS);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> recall.process(aggs));
        assertThat(e.getMessage(), containsString("Cardinality of field [foo] is too high"));
    }
}
