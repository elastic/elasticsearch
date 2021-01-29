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
package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.client.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.client.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.client.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClassificationTests extends AbstractXContentTestCase<Classification> {

    public static Classification randomClassification() {
        return Classification.builder(randomAlphaOfLength(10))
            .setLambda(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setGamma(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setEta(randomBoolean() ? null : randomDoubleBetween(0.001, 1.0, true))
            .setMaxTrees(randomBoolean() ? null : randomIntBetween(1, 2000))
            .setFeatureBagFraction(randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false))
            .setNumTopFeatureImportanceValues(randomBoolean() ? null : randomIntBetween(0, Integer.MAX_VALUE))
            .setPredictionFieldName(randomBoolean() ? null : randomAlphaOfLength(10))
            .setTrainingPercent(randomBoolean() ? null : randomDoubleBetween(1.0, 100.0, true))
            .setRandomizeSeed(randomBoolean() ? null : randomLong())
            .setClassAssignmentObjective(randomBoolean() ? null : randomFrom(Classification.ClassAssignmentObjective.values()))
            .setNumTopClasses(randomBoolean() ? null : randomIntBetween(-1, 1000))
            .setFeatureProcessors(randomBoolean() ? null :
                Stream.generate(() -> randomFrom(FrequencyEncodingTests.createRandom(),
                    OneHotEncodingTests.createRandom(),
                    TargetMeanEncodingTests.createRandom()))
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList()))
            .setAlpha(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setEtaGrowthRatePerTree(randomBoolean() ? null : randomDoubleBetween(0.5, 2.0, true))
            .setSoftTreeDepthLimit(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setSoftTreeDepthTolerance(randomBoolean() ? null : randomDoubleBetween(0.01, Double.MAX_VALUE, true))
            .setDownsampleFactor(randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false))
            .setMaxOptimizationRoundsPerHyperparameter(randomBoolean() ? null : randomIntBetween(0, 20))
            .build();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.startsWith("feature_processors");
    }

    @Override
    protected Classification createTestInstance() {
        return randomClassification();
    }

    @Override
    protected Classification doParseInstance(XContentParser parser) throws IOException {
        return Classification.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }
}
