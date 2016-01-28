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

package org.elasticsearch.search.suggest;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractSuggestionBuilderTestCase<SB extends SuggestionBuilder<SB>> extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    protected static NamedWriteableRegistry namedWriteableRegistry;
    private static final Suggesters suggesters = new Suggesters(Collections.emptyMap(), null, null);

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, TermSuggestionBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, PhraseSuggestionBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, CompletionSuggestionBuilder.PROTOTYPE);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }


    /**
     * Test serialization and deserialization of the suggestion builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB original = randomTestBuilder();
            SB deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    /**
     * returns a random suggestion builder, setting the common options randomly
     */
    protected SB randomTestBuilder() {
        SB randomSuggestion = randomSuggestionBuilder();
        maybeSet(randomSuggestion::text, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::prefix, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::regex, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::field, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::analyzer, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::size, randomIntBetween(1, 20));
        maybeSet(randomSuggestion::shardSize, randomInt(20));
        return randomSuggestion;
    }

    /**
     * create a randomized {@link SuggestBuilder} that is used in further tests
     */
    protected abstract SB randomSuggestionBuilder();

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB firstBuilder = randomTestBuilder();
            assertFalse("suggestion builder is equal to null", firstBuilder.equals(null));
            assertFalse("suggestion builder is equal to incompatible type", firstBuilder.equals(""));
            assertTrue("suggestion builder is not equal to self", firstBuilder.equals(firstBuilder));
            assertThat("same suggestion builder's hashcode returns different values if called multiple times", firstBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));
            assertThat("different suggestion builders should not be equal", mutate(firstBuilder), not(equalTo(firstBuilder)));

            SB secondBuilder = serializedCopy(firstBuilder);
            assertTrue("suggestion builder is not equal to self", secondBuilder.equals(secondBuilder));
            assertTrue("suggestion builder is not equal to its copy", firstBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", secondBuilder.equals(firstBuilder));
            assertThat("suggestion builder copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));

            SB thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("suggestion builder is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("suggestion builder is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("suggestion builder copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
            assertThat("suggestion builder copy's hashcode is different from original hashcode", firstBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstBuilder));
        }
    }

    /**
     *  creates random suggestion builder, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        QueryParseContext context = new QueryParseContext(null);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB suggestionBuilder = randomTestBuilder();
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                xContentBuilder.prettyPrint();
            }
            xContentBuilder.startObject();
            suggestionBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            xContentBuilder.endObject();

            XContentParser parser = XContentHelper.createParser(xContentBuilder.bytes());
            context.reset(parser);
            // we need to skip the start object and the name, those will be parsed by outer SuggestBuilder
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();

            SuggestionBuilder<?> secondSuggestionBuilder = SuggestionBuilder.fromXContent(context, suggestionBuilder.name(), suggesters);
            assertNotSame(suggestionBuilder, secondSuggestionBuilder);
            assertEquals(suggestionBuilder, secondSuggestionBuilder);
            assertEquals(suggestionBuilder.hashCode(), secondSuggestionBuilder.hashCode());
        }
    }

    private SB mutate(SB firstBuilder) throws IOException {
        SB mutation = serializedCopy(firstBuilder);
        assertNotSame(mutation, firstBuilder);
        if (randomBoolean()) {
            // change one of the common SuggestionBuilder parameters
            switch (randomIntBetween(0, 6)) {
            case 0:
                mutation.text(randomValueOtherThan(mutation.text(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 1:
                mutation.prefix(randomValueOtherThan(mutation.prefix(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 2:
                mutation.regex(randomValueOtherThan(mutation.regex(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 3:
                mutation.field(randomValueOtherThan(mutation.field(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 4:
                mutation.analyzer(randomValueOtherThan(mutation.analyzer(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 5:
                mutation.size(randomValueOtherThan(mutation.size(), () -> randomIntBetween(1, 20)));
                break;
            case 6:
                mutation.shardSize(randomValueOtherThan(mutation.shardSize(), () -> randomIntBetween(1, 20)));
                break;
            }
        } else {
            mutateSpecificParameters(firstBuilder);
        }
        return mutation;
    }

    /**
     * take and input {@link SuggestBuilder} and return another one that is different in one aspect (to test non-equality)
     */
    protected abstract void mutateSpecificParameters(SB firstBuilder) throws IOException;

    @SuppressWarnings("unchecked")
    protected SB serializedCopy(SB original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeSuggestion(original);;
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                return (SB) in.readSuggestion();
            }
        }
    }

    protected static <T> void maybeSet(Consumer<T> consumer, T value) {
        if (randomBoolean()) {
            consumer.accept(value);
        }
    }

    /**
     * helper to get a random value in a certain range that's different from the input
     */
    protected static <T> T randomValueOtherThan(T input, Supplier<T> randomSupplier) {
        T randomValue = null;
        do {
            randomValue = randomSupplier.get();
        } while (randomValue.equals(input));
        return randomValue;
    }

}
