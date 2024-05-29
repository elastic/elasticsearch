/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.suggest.completion.CompletionSuggesterBuilderTests;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilderTests;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilderTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map.Entry;

import static java.util.Collections.emptyList;

public class SuggestBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_RUNS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static NamedXContentRegistry xContentRegistry;

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() {
        namedWriteableRegistry = null;
        xContentRegistry = null;
    }

    /**
     *  creates random suggestion builder, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            SuggestBuilder suggestBuilder = randomSuggestBuilder();
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                xContentBuilder.prettyPrint();
            }
            suggestBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(xContentBuilder)) {
                SuggestBuilder secondSuggestBuilder = SuggestBuilder.fromXContent(parser);
                assertNotSame(suggestBuilder, secondSuggestBuilder);
                assertEquals(suggestBuilder, secondSuggestBuilder);
                assertEquals(suggestBuilder.hashCode(), secondSuggestBuilder.hashCode());
            }
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            // explicit about type parameters, see: https://bugs.eclipse.org/bugs/show_bug.cgi?id=481649
            EqualsHashCodeTestUtils.<SuggestBuilder>checkEqualsAndHashCode(randomSuggestBuilder(), original -> {
                return copyWriteable(original, namedWriteableRegistry, SuggestBuilder::new);
            }, this::createMutation);
        }
    }

    /**
     * Test serialization and deserialization
     */
    public void testSerialization() throws IOException {
        for (int i = 0; i < NUMBER_OF_RUNS; i++) {
            SuggestBuilder suggestBuilder = randomSuggestBuilder();
            SuggestBuilder deserializedModel = copyWriteable(suggestBuilder, namedWriteableRegistry, SuggestBuilder::new);
            assertEquals(suggestBuilder, deserializedModel);
            assertEquals(suggestBuilder.hashCode(), deserializedModel.hashCode());
            assertNotSame(suggestBuilder, deserializedModel);
        }
    }

    public void testIllegalSuggestionName() {
        try {
            new SuggestBuilder().addSuggestion(null, PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder());
            fail("exception expected");
        } catch (NullPointerException e) {
            assertEquals("every suggestion needs a name", e.getMessage());
        }

        try {
            new SuggestBuilder().addSuggestion("my-suggest", PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder())
                .addSuggestion("my-suggest", PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder());
            fail("exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("already added another suggestion with name [my-suggest]", e.getMessage());
        }
    }

    protected SuggestBuilder createMutation(SuggestBuilder original) throws IOException {
        SuggestBuilder mutation = new SuggestBuilder().setGlobalText(original.getGlobalText());
        for (Entry<String, SuggestionBuilder<?>> suggestionBuilder : original.getSuggestions().entrySet()) {
            mutation.addSuggestion(suggestionBuilder.getKey(), suggestionBuilder.getValue());
        }
        if (randomBoolean()) {
            mutation.setGlobalText(randomAlphaOfLengthBetween(5, 60));
        } else {
            mutation.addSuggestion(randomAlphaOfLength(10), PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder());
        }
        return mutation;
    }

    public static SuggestBuilder randomSuggestBuilder() {
        SuggestBuilder builder = new SuggestBuilder();
        if (randomBoolean()) {
            builder.setGlobalText(randomAlphaOfLengthBetween(1, 20));
        }
        final int numSuggestions = randomIntBetween(1, 5);
        for (int i = 0; i < numSuggestions; i++) {
            builder.addSuggestion(randomAlphaOfLengthBetween(5, 10), randomSuggestionBuilder());
        }
        return builder;
    }

    private static SuggestionBuilder<?> randomSuggestionBuilder() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> TermSuggestionBuilderTests.randomTermSuggestionBuilder();
            case 1 -> PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder();
            case 2 -> CompletionSuggesterBuilderTests.randomCompletionSuggestionBuilder();
            default -> TermSuggestionBuilderTests.randomTermSuggestionBuilder();
        };
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
