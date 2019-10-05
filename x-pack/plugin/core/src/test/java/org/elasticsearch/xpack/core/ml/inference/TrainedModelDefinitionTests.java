/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TrainedModelDefinitionTests extends AbstractSerializingTestCase<TrainedModelDefinition> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TrainedModelDefinition doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelDefinition.fromXContent(parser, lenient).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    public static TrainedModelDefinition.Builder createRandomBuilder() {
        int numberOfProcessors = randomIntBetween(1, 10);
        return new TrainedModelDefinition.Builder()
            .setPreProcessors(
                randomBoolean() ? null :
                    Stream.generate(() -> randomFrom(FrequencyEncodingTests.createRandom(),
                        OneHotEncodingTests.createRandom(),
                        TargetMeanEncodingTests.createRandom()))
                        .limit(numberOfProcessors)
                        .collect(Collectors.toList()))
            .setTrainedModel(randomFrom(TreeTests.createRandom()));
    }
    @Override
    protected TrainedModelDefinition createTestInstance() {
        return createRandomBuilder().build();
    }

    @Override
    protected Writeable.Reader<TrainedModelDefinition> instanceReader() {
        return TrainedModelDefinition::new;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

}
