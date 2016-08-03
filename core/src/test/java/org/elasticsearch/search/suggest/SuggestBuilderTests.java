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
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.suggest.completion.CompletionSuggesterBuilderTests;
import org.elasticsearch.search.suggest.completion.WritableTestCase;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilderTests;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilderTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map.Entry;

import static java.util.Collections.emptyList;

public class SuggestBuilderTests extends WritableTestCase<SuggestBuilder> {

    private static NamedWriteableRegistry namedWriteableRegistry;
    private static Suggesters suggesters;

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        suggesters = searchModule.getSuggesters();
    }

    @AfterClass
    public static void afterClass() {
        namedWriteableRegistry = null;
        suggesters = null;
    }

    @Override
    protected NamedWriteableRegistry provideNamedWritableRegistry() {
        return namedWriteableRegistry;
    }

    /**
     *  creates random suggestion builder, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            SuggestBuilder suggestBuilder = createTestModel();
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                xContentBuilder.prettyPrint();
            }
            suggestBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            XContentParser parser = XContentHelper.createParser(xContentBuilder.bytes());
            QueryParseContext context = new QueryParseContext(new IndicesQueriesRegistry(), parser, ParseFieldMatcher.STRICT);
            SuggestBuilder secondSuggestBuilder = SuggestBuilder.fromXContent(context, suggesters);
            assertNotSame(suggestBuilder, secondSuggestBuilder);
            assertEquals(suggestBuilder, secondSuggestBuilder);
            assertEquals(suggestBuilder.hashCode(), secondSuggestBuilder.hashCode());
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

    @Override
    protected SuggestBuilder createTestModel() {
        return randomSuggestBuilder();
    }

    @Override
    protected SuggestBuilder createMutation(SuggestBuilder original) throws IOException {
        SuggestBuilder mutation = new SuggestBuilder().setGlobalText(original.getGlobalText());
        for (Entry<String, SuggestionBuilder<?>> suggestionBuilder : original.getSuggestions().entrySet()) {
            mutation.addSuggestion(suggestionBuilder.getKey(), suggestionBuilder.getValue());
        }
        if (randomBoolean()) {
            mutation.setGlobalText(randomAsciiOfLengthBetween(5, 60));
        } else {
            mutation.addSuggestion(randomAsciiOfLength(10), PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder());
        }
        return mutation;
    }

    @Override
    protected SuggestBuilder readFrom(StreamInput in) throws IOException {
        return new SuggestBuilder(in);
    }

    public static SuggestBuilder randomSuggestBuilder() {
        SuggestBuilder builder = new SuggestBuilder();
        if (randomBoolean()) {
            builder.setGlobalText(randomAsciiOfLengthBetween(1, 20));
        }
        final int numSuggestions = randomIntBetween(1, 5);
        for (int i = 0; i < numSuggestions; i++) {
            builder.addSuggestion(randomAsciiOfLengthBetween(5, 10), randomSuggestionBuilder());
        }
        return builder;
    }

    private static SuggestionBuilder<?> randomSuggestionBuilder() {
        switch (randomIntBetween(0, 2)) {
            case 0: return TermSuggestionBuilderTests.randomTermSuggestionBuilder();
            case 1: return PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder();
            case 2: return CompletionSuggesterBuilderTests.randomCompletionSuggestionBuilder();
            default: return TermSuggestionBuilderTests.randomTermSuggestionBuilder();
        }
    }

}
