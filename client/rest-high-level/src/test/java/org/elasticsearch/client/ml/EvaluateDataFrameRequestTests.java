/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.QueryConfig;
import org.elasticsearch.client.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.ClassificationTests;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.OutlierDetectionTests;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RegressionTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static java.util.function.Predicate.not;

public class EvaluateDataFrameRequestTests extends AbstractXContentTestCase<EvaluateDataFrameRequest> {

    public static EvaluateDataFrameRequest createRandom() {
        int indicesCount = randomIntBetween(1, 5);
        List<String> indices = new ArrayList<>(indicesCount);
        for (int i = 0; i < indicesCount; i++) {
            indices.add(randomAlphaOfLength(10));
        }
        QueryConfig queryConfig = randomBoolean()
            ? new QueryConfig(QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)))
            : null;
        Evaluation evaluation =
            randomFrom(OutlierDetectionTests.createRandom(), ClassificationTests.createRandom(), RegressionTests.createRandom());
        return new EvaluateDataFrameRequest(indices, queryConfig, evaluation);
    }

    @Override
    protected EvaluateDataFrameRequest createTestInstance() {
        return createRandom();
    }

    @Override
    protected EvaluateDataFrameRequest doParseInstance(XContentParser parser) throws IOException {
        return EvaluateDataFrameRequest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in root only
        return not(String::isEmpty);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        namedXContent.addAll(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }
}
