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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
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
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.completion.WritableTestCase;
import org.elasticsearch.search.suggest.phrase.Laplace;
import org.elasticsearch.search.suggest.phrase.LinearInterpolation;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilderTests;
import org.elasticsearch.search.suggest.phrase.SmoothingModel;
import org.elasticsearch.search.suggest.phrase.StupidBackoff;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilderTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map.Entry;

import static org.hamcrest.Matchers.containsString;

public class SuggestBuilderTests extends WritableTestCase<SuggestBuilder> {

    private static NamedWriteableRegistry namedWriteableRegistry;
    private static IndicesQueriesRegistry queriesRegistry;
    private static ParseFieldMatcher parseFieldMatcher;
    private static Suggesters suggesters;

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() {
        NamedWriteableRegistry nwRegistry = new NamedWriteableRegistry();
        nwRegistry.registerPrototype(SuggestionBuilder.class, TermSuggestionBuilder.PROTOTYPE);
        nwRegistry.registerPrototype(SuggestionBuilder.class, PhraseSuggestionBuilder.PROTOTYPE);
        nwRegistry.registerPrototype(SuggestionBuilder.class, CompletionSuggestionBuilder.PROTOTYPE);
        nwRegistry.registerPrototype(SmoothingModel.class, Laplace.PROTOTYPE);
        nwRegistry.registerPrototype(SmoothingModel.class, LinearInterpolation.PROTOTYPE);
        nwRegistry.registerPrototype(SmoothingModel.class, StupidBackoff.PROTOTYPE);
        namedWriteableRegistry = nwRegistry;
        queriesRegistry = new SearchModule(Settings.EMPTY, namedWriteableRegistry).buildQueryParserRegistry();
        suggesters = new Suggesters(new HashMap<>());
        parseFieldMatcher = ParseFieldMatcher.STRICT;
    }

    @AfterClass
    public static void afterClass() {
        namedWriteableRegistry = null;
        queriesRegistry = null;
        suggesters = null;
        parseFieldMatcher = null;
    }

    @Override
    protected NamedWriteableRegistry provideNamedWritableRegistry() {
        return namedWriteableRegistry;
    }

    /**
     * Test that valid JSON suggestion request passes.
     */
    public void testValidJsonRequestPayload() throws Exception {
        final String field = RandomStrings.randomAsciiOfLength(getRandom(), 10).toLowerCase(Locale.ROOT);
        String payload = "{\n" +
                         "  \"valid-suggestion\" : {\n" +
                         "    \"text\" : \"the amsterdma meetpu\",\n" +
                         "    \"term\" : {\n" +
                         "      \"field\" : \"" + field + "\"\n" +
                         "    }\n" +
                         "  }\n" +
                         "}";
        try {
            final SuggestBuilder suggestBuilder = SuggestBuilder.fromXContent(newParseContext(payload), suggesters, true);
            assertNotNull(suggestBuilder);
        } catch (Exception e) {
            fail("Parsing valid json should not have thrown exception: " + e.getMessage());
        }
    }

    /**
     * Test that a malformed JSON suggestion request fails.
     */
    public void testMalformedJsonRequestPayload() throws Exception {
        final String field = RandomStrings.randomAsciiOfLength(getRandom(), 10).toLowerCase(Locale.ROOT);
        // {"bad-payload":{"prefix":"sug","completion":{"field":"ytnahgylcc","payload":[{"payload":"field"}]}}}
        String payload = "{\n" +
                         "  \"bad-payload\" : {\n" +
                         "    \"text\" : \"the amsterdma meetpu\",\n" +
                         "    \"term\" : {\n" +
                         "      \"field\" : { \"" + field + "\" : \"bad-object\" }\n" +
                         "    }\n" +
                         "  }\n" +
                         "}";
        try {
            final SuggestBuilder suggestBuilder = SuggestBuilder.fromXContent(newParseContext(payload), suggesters, true);
            fail("Should not have been able to create SuggestBuilder from malformed JSON: " + suggestBuilder);
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("parsing failed"));
        }

        // nocommit TODO: awaits completion suggester
        /*payload = "{\n" +
                  "  \"bad-payload\" : { \n" +
                  "    \"prefix\" : \"sug\",\n" +
                  "    \"completion\" : { \n" +
                  "      \"field\" : \"" + field + "\",\n " +
                  "      \"payload\" : [ {\"payload\":\"field\"} ]\n" +
                  "    }\n" +
                  "  }\n" +
                  "}\n";
        try {
            final SuggestBuilder suggestBuilder = SuggestBuilder.fromXContent(newParseContext(payload), suggesters);
            fail("Should not have been able to create SuggestBuilder from malformed JSON: " + suggestBuilder);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("encountered invalid token"));
        }*/
    }

    /**
     *  creates random suggestion builder, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        Suggesters suggesters = new Suggesters(Collections.emptyMap());
        QueryParseContext context = new QueryParseContext(null);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            SuggestBuilder suggestBuilder = createTestModel();
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                xContentBuilder.prettyPrint();
            }
            suggestBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            XContentParser parser = XContentHelper.createParser(xContentBuilder.bytes());
            context.reset(parser);

            SuggestBuilder secondSuggestBuilder = SuggestBuilder.fromXContent(context, suggesters, true);
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
        return SuggestBuilder.PROTOTYPE.readFrom(in);
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
            //norelease TODO: uncomment case 2: return CompletionSuggesterBuilderTests.randomCompletionSuggestionBuilder();
            default: return TermSuggestionBuilderTests.randomTermSuggestionBuilder();
        }
    }

    private static QueryParseContext newParseContext(final String xcontent) throws IOException {
        final QueryParseContext parseContext = new QueryParseContext(queriesRegistry);
        parseContext.reset(XContentFactory.xContent(xcontent).createParser(xcontent));
        parseContext.parseFieldMatcher(parseFieldMatcher);
        return parseContext;
    }

}
