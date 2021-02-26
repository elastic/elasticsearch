/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MultiTests extends AbstractXContentTestCase<Multi> {

    @Override
    protected Multi doParseInstance(XContentParser parser) throws IOException {
        return Multi.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Multi createTestInstance() {
        return createRandom();
    }

    public static Multi createRandom() {
        final List<PreProcessor> processors;
        Boolean isCustom = randomBoolean() ? null : randomBoolean();
        if (isCustom == null || isCustom == false) {
            NGram nGram = new NGram(randomAlphaOfLength(10), Arrays.asList(1, 2), 0, 10, isCustom, "f");
            List<PreProcessor> preProcessorList = new ArrayList<>();
            preProcessorList.add(nGram);
            Stream.generate(() -> randomFrom(
                FrequencyEncodingTests.createRandom(randomFrom(nGram.outputFields())),
                TargetMeanEncodingTests.createRandom(randomFrom(nGram.outputFields())),
                OneHotEncodingTests.createRandom(randomFrom(nGram.outputFields()))
            )).limit(randomIntBetween(1, 10)).forEach(preProcessorList::add);
            processors = preProcessorList;
        } else {
            processors = Stream.generate(
                () -> randomFrom(
                    FrequencyEncodingTests.createRandom(),
                    TargetMeanEncodingTests.createRandom(),
                    OneHotEncodingTests.createRandom(),
                    NGramTests.createRandom()
                )
            ).limit(randomIntBetween(2, 10)).collect(Collectors.toList());
        }
        return new Multi(processors, isCustom);
    }

}
