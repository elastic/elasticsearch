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

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.search.suggest.SuggestionBuilderTestCase;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PhraseSuggestionBuilderTests extends SuggestionBuilderTestCase<PhraseSuggestionBuilder> {

    @Override
    protected PhraseSuggestionBuilder randomSuggestionBuilder() {
        PhraseSuggestionBuilder testBuilder = new PhraseSuggestionBuilder(randomAsciiOfLength(10));
        maybeSet(testBuilder::maxErrors, randomFloat());
        maybeSet(testBuilder::separator, randomAsciiOfLengthBetween(1, 10));
        maybeSet(testBuilder::realWordErrorLikelihood, randomFloat());
        maybeSet(testBuilder::confidence, randomFloat());
        maybeSet(testBuilder::collatePrune, randomBoolean());
        maybeSet(testBuilder::collateQuery, randomAsciiOfLengthBetween(3, 20));
        if (randomBoolean()) {
            // preTag, postTag
            testBuilder.highlight(randomAsciiOfLengthBetween(3, 20), randomAsciiOfLengthBetween(3, 20));
        }
        maybeSet(testBuilder::gramSize, randomIntBetween(1, 5));
        maybeSet(testBuilder::forceUnigrams, randomBoolean());
        maybeSet(testBuilder::tokenLimit, randomInt(20));
        if (randomBoolean()) {
            // TODO add random model
        }
        if (randomBoolean()) {
            // TODO add random collate parameter
        }
        if (randomBoolean()) {
            // TODO add random generator
        }
        return testBuilder;
    }

    private static <T> void maybeSet(Consumer<T> consumer, T value) {
        if (randomBoolean()) {
            consumer.accept(value);
        }
    }

    @Override
    protected PhraseSuggestionBuilder mutate(PhraseSuggestionBuilder original) throws IOException {
        PhraseSuggestionBuilder mutation = serializedCopy(original);
        switch (randomIntBetween(0, 4)) {
        case 0:
            mutation.maxErrors(randomValueOtherThan(original.maxErrors(), () -> randomFloat()));
            break;
        case 1:
            mutation.realWordErrorLikelihood(randomValueOtherThan(original.realWordErrorLikelihood(), () -> randomFloat()));
            break;
        case 2:
            mutation.confidence(randomValueOtherThan(original.confidence(), () -> randomFloat()));
            break;
        case 3:
            mutation.gramSize(randomValueOtherThan(original.gramSize(), () -> randomIntBetween(1, 5)));
            break;
        case 4:
            mutation.tokenLimit(randomValueOtherThan(original.tokenLimit(), () -> randomInt(20)));
            break;
        // TODO mutate all settings
        }
        return mutation;
    }

    /**
     * helper to get a random value in a certain range that's different from the input
     */
    private static <T> T randomValueOtherThan(T input, Supplier<T> randomSupplier) {
        T randomValue = null;
        do {
            randomValue = randomSupplier.get();
        } while (randomValue.equals(input));
        return randomValue;
    }

}
