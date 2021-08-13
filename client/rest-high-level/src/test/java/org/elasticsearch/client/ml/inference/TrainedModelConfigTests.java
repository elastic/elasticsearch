/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
            randomBoolean() ? null : randomFrom(TrainedModelType.values()),
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
                RegressionConfigTests.randomRegressionConfig(),
            randomBoolean() ? null : IndexLocationTests.randomInstance());
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
        return field -> field.isEmpty() == false;
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
