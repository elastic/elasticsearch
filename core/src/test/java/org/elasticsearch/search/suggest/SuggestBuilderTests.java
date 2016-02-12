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
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.completion.WritableTestCase;
import org.elasticsearch.search.suggest.phrase.Laplace;
import org.elasticsearch.search.suggest.phrase.LinearInterpolation;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilderTests;
import org.elasticsearch.search.suggest.phrase.SmoothingModel;
import org.elasticsearch.search.suggest.phrase.StupidBackoff;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.IOException;
import java.util.Collections;

public class SuggestBuilderTests extends WritableTestCase<SuggestBuilder> {


    @Override
    protected NamedWriteableRegistry provideNamedWritableRegistry() {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, TermSuggestionBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, PhraseSuggestionBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, CompletionSuggestionBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SmoothingModel.class, Laplace.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SmoothingModel.class, LinearInterpolation.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SmoothingModel.class, StupidBackoff.PROTOTYPE);
        return namedWriteableRegistry;
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
            parser.nextToken();

            SuggestBuilder secondSuggestBuilder = SuggestBuilder.fromXContent(context, suggesters);
            assertNotSame(suggestBuilder, secondSuggestBuilder);
            assertEquals(suggestBuilder, secondSuggestBuilder);
            assertEquals(suggestBuilder.hashCode(), secondSuggestBuilder.hashCode());
        }
    }

    @Override
    protected SuggestBuilder createTestModel() {
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        if (randomBoolean()) {
            suggestBuilder.setGlobalText(randomAsciiOfLengthBetween(5, 50));
        }
        int numberOfSuggestions = randomIntBetween(0, 5);
        for (int i = 0; i < numberOfSuggestions; i++) {
            suggestBuilder.addSuggestion(PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder());
        }
        return suggestBuilder;
    }

    @Override
    protected SuggestBuilder createMutation(SuggestBuilder original) throws IOException {
        SuggestBuilder mutation = new SuggestBuilder().setGlobalText(original.getGlobalText());
        for (SuggestionBuilder<?> suggestionBuilder : original.getSuggestions()) {
            mutation.addSuggestion(suggestionBuilder);
        }
        if (randomBoolean()) {
            mutation.setGlobalText(randomAsciiOfLengthBetween(5, 60));
        } else {
            mutation.addSuggestion(PhraseSuggestionBuilderTests.randomPhraseSuggestionBuilder());
        }
        return mutation;
    }

    @Override
    protected SuggestBuilder readFrom(StreamInput in) throws IOException {
        return SuggestBuilder.PROTOTYPE.readFrom(in);
    }

}
