/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.client.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.client.ml.inference.preprocessing.MultiTests;
import org.elasticsearch.client.ml.inference.preprocessing.NGramTests;
import org.elasticsearch.client.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.client.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.client.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.EnsembleTests;
import org.elasticsearch.client.ml.inference.trainedmodel.tree.TreeTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TrainedModelDefinitionTests extends AbstractXContentTestCase<TrainedModelDefinition> {

    @Override
    protected TrainedModelDefinition doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelDefinition.fromXContent(parser).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    public static TrainedModelDefinition.Builder createRandomBuilder() {
        return createRandomBuilder(randomFrom(TargetType.values()));
    }

    public static TrainedModelDefinition.Builder createRandomBuilder(TargetType targetType) {
        int numberOfProcessors = randomIntBetween(1, 10);
        return new TrainedModelDefinition.Builder()
            .setPreProcessors(
                randomBoolean() ? null :
                    Stream.generate(() -> randomFrom(
                        FrequencyEncodingTests.createRandom(),
                        OneHotEncodingTests.createRandom(),
                        TargetMeanEncodingTests.createRandom(),
                        NGramTests.createRandom(),
                        MultiTests.createRandom()))
                        .limit(numberOfProcessors)
                        .collect(Collectors.toList()))
            .setTrainedModel(randomFrom(TreeTests.buildRandomTree(Arrays.asList("foo", "bar"), 6, targetType),
                EnsembleTests.createRandom(targetType)));
    }

    @Override
    protected TrainedModelDefinition createTestInstance() {
        return createRandomBuilder().build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

}
