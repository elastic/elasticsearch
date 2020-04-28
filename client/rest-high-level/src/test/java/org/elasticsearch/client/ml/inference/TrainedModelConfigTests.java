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
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.Version;
import org.elasticsearch.client.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.client.ml.inference.trainedmodel.RegressionConfigTests;
import org.elasticsearch.client.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TrainedModelConfigTests extends AbstractXContentTestCase<TrainedModelConfig> {

    public static TrainedModelConfig createTestTrainedModelConfig() {
        TargetType targetType = randomFrom(TargetType.values());
        return new TrainedModelConfig(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Version.CURRENT,
            randomBoolean() ? null : randomAlphaOfLength(100),
            Instant.ofEpochMilli(randomNonNegativeLong()),
            randomBoolean() ? null : TrainedModelDefinitionTests.createRandomBuilder(targetType).build(),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null :
                Stream.generate(() -> randomAlphaOfLength(10)).limit(randomIntBetween(0, 5)).collect(Collectors.toList()),
            randomBoolean() ? null : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomBoolean() ? null : TrainedModelInputTests.createRandomInput(),
            randomBoolean() ? null : randomNonNegativeLong(),
            randomBoolean() ? null : randomNonNegativeLong(),
            randomBoolean() ? null : randomFrom("platinum", "basic"),
            randomBoolean() ? null :
                Stream.generate(() -> randomAlphaOfLength(10))
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toMap(Function.identity(), (k) -> randomAlphaOfLength(10))),
            targetType.equals(TargetType.CLASSIFICATION) ?
                ClassificationConfigTests.randomClassificationConfig() :
                RegressionConfigTests.randomRegressionConfig());
    }

    @Override
    protected TrainedModelConfig doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected TrainedModelConfig createTestInstance() {
        return createTestTrainedModelConfig();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

}
