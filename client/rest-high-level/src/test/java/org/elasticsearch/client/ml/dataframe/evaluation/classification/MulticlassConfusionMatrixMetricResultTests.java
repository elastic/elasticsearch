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
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MulticlassConfusionMatrixMetricResultTests extends AbstractXContentTestCase<MulticlassConfusionMatrixMetric.Result> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected MulticlassConfusionMatrixMetric.Result createTestInstance() {
        int numClasses = randomIntBetween(2, 100);
        List<String> classNames = Stream.generate(() -> randomAlphaOfLength(10)).limit(numClasses).collect(Collectors.toList());
        Map<String, Map<String, Long>> confusionMatrix = new TreeMap<>();
        for (int i = 0; i < numClasses; i++) {
            Map<String, Long> row = new TreeMap<>();
            confusionMatrix.put(classNames.get(i), row);
            for (int j = 0; j < numClasses; j++) {
                if (randomBoolean()) {
                    row.put(classNames.get(i), randomNonNegativeLong());
                }
            }
        }
        long otherClassesCount = randomNonNegativeLong();
        return new MulticlassConfusionMatrixMetric.Result(confusionMatrix, otherClassesCount);
    }

    @Override
    protected MulticlassConfusionMatrixMetric.Result doParseInstance(XContentParser parser) throws IOException {
        return MulticlassConfusionMatrixMetric.Result.fromXContent(parser);
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
