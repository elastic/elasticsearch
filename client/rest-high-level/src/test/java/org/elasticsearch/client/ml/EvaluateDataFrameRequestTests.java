/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.QueryConfig;
import org.elasticsearch.client.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RegressionTests;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.BinarySoftClassificationTests;
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
        Evaluation evaluation = randomBoolean() ? BinarySoftClassificationTests.createRandom() : RegressionTests.createRandom();
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
