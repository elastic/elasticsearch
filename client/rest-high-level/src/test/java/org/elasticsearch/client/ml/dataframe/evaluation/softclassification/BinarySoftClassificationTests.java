/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class BinarySoftClassificationTests extends AbstractXContentTestCase<BinarySoftClassification> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }

    public static BinarySoftClassification createRandom() {
        List<EvaluationMetric> metrics = new ArrayList<>();
        if (randomBoolean()) {
            metrics.add(new AucRocMetric(randomBoolean()));
        }
        if (randomBoolean()) {
            metrics.add(new PrecisionMetric(Arrays.asList(randomArray(1,
                4,
                Double[]::new,
                BinarySoftClassificationTests::randomDouble))));
        }
        if (randomBoolean()) {
            metrics.add(new RecallMetric(Arrays.asList(randomArray(1,
                4,
                Double[]::new,
                BinarySoftClassificationTests::randomDouble))));
        }
        if (randomBoolean()) {
            metrics.add(new ConfusionMatrixMetric(Arrays.asList(randomArray(1,
                4,
                Double[]::new,
                BinarySoftClassificationTests::randomDouble))));
        }
        return randomBoolean() ?
            new BinarySoftClassification(randomAlphaOfLength(10), randomAlphaOfLength(10)) :
            new BinarySoftClassification(randomAlphaOfLength(10), randomAlphaOfLength(10), metrics.isEmpty() ? null : metrics);
    }

    @Override
    protected BinarySoftClassification createTestInstance() {
        return createRandom();
    }

    @Override
    protected BinarySoftClassification doParseInstance(XContentParser parser) throws IOException {
        return BinarySoftClassification.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> !field.isEmpty();
    }

}
