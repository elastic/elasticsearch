/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class OutlierDetectionTests extends AbstractSerializingTestCase<OutlierDetection> {

    private static final EvaluationParameters EVALUATION_PARAMETERS = new EvaluationParameters(100);

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(MlEvaluationNamedXContentProvider.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }

    public static OutlierDetection createRandom() {
        List<EvaluationMetric> metrics = new ArrayList<>();
        if (randomBoolean()) {
            metrics.add(AucRocTests.createRandom());
        }
        if (randomBoolean()) {
            metrics.add(PrecisionTests.createRandom());
        }
        if (randomBoolean()) {
            metrics.add(RecallTests.createRandom());
        }
        if (randomBoolean()) {
            metrics.add(ConfusionMatrixTests.createRandom());
        }
        if (metrics.isEmpty()) {
            // not a good day to play in the lottery; let's add them all
            metrics.add(AucRocTests.createRandom());
            metrics.add(PrecisionTests.createRandom());
            metrics.add(RecallTests.createRandom());
            metrics.add(ConfusionMatrixTests.createRandom());
        }
        return new OutlierDetection(randomAlphaOfLength(10), randomAlphaOfLength(10), metrics);
    }

    @Override
    protected OutlierDetection doParseInstance(XContentParser parser) throws IOException {
        return OutlierDetection.fromXContent(parser);
    }

    @Override
    protected OutlierDetection createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<OutlierDetection> instanceReader() {
        return OutlierDetection::new;
    }

    public void testConstructor_GivenEmptyMetrics() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new OutlierDetection("foo", "bar", Collections.emptyList()));
        assertThat(e.getMessage(), equalTo("[outlier_detection] must have one or more metrics"));
    }

    public void testBuildSearch() {
        QueryBuilder userProvidedQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("field_A", "some-value"))
                .filter(QueryBuilders.termQuery("field_B", "some-other-value"));
        QueryBuilder expectedSearchQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.existsQuery("act"))
                .filter(QueryBuilders.existsQuery("prob"))
                .filter(QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("field_A", "some-value"))
                    .filter(QueryBuilders.termQuery("field_B", "some-other-value")));

        OutlierDetection evaluation = new OutlierDetection("act", "prob", Arrays.asList(new Precision(Arrays.asList(0.7))));

        SearchSourceBuilder searchSourceBuilder = evaluation.buildSearch(EVALUATION_PARAMETERS, userProvidedQuery);
        assertThat(searchSourceBuilder.query(), equalTo(expectedSearchQuery));
        assertThat(searchSourceBuilder.aggregations().count(), greaterThan(0));
    }
}
