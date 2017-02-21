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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;

public class SuggestTests extends ESTestCase {

    public static Suggest createTestItem() {
        int numEntries = randomIntBetween(0, 5);
        List<Suggestion<? extends Entry<? extends Option>>> suggestions = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            suggestions.add(SuggestionTests.createTestItem());
        }
        return new Suggest(suggestions);
    }

    public void testFromXContent() throws IOException {
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        Suggest suggest = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toXContent(suggest, xContentType, params, humanReadable);
        Suggest parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureFieldName(parser, parser.nextToken(), Suggest.NAME);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            parsed = Suggest.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(suggest.size(), parsed.size());
        for (Suggestion suggestion : suggest) {
            Suggestion<? extends Entry<? extends Option>> parsedSuggestion = parsed.getSuggestion(suggestion.getName());
            assertNotNull(parsedSuggestion);
            assertEquals(suggestion.getClass(), parsedSuggestion.getClass());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, params, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Option option = new Option(new Text("someText"), new Text("somethingHighlighted"), 1.3f, true);
        Entry<Option> entry = new Entry<>(new Text("entryText"), 42, 313);
        entry.addOption(option);
        Suggestion<Entry<Option>> suggestion = new Suggestion<>("suggestionName", 5);
        suggestion.addTerm(entry);
        Suggest suggest = new Suggest(Collections.singletonList(suggestion));
        BytesReference xContent = toXContent(suggest, XContentType.JSON, randomBoolean());
        assertEquals(
                "{\"suggest\":"
                        + "{\"suggestionName\":"
                            + "[{\"text\":\"entryText\","
                            + "\"offset\":42,"
                            + "\"length\":313,"
                            + "\"options\":[{\"text\":\"someText\","
                                        + "\"highlighted\":\"somethingHighlighted\","
                                        + "\"score\":1.3,"
                                        + "\"collate_match\":true}]"
                            + "}]"
                        + "}"
                +"}",
                xContent.utf8ToString());
    }

    public void testFilter() throws Exception {
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions;
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(randomAsciiOfLength(10), 2);
        PhraseSuggestion phraseSuggestion = new PhraseSuggestion(randomAsciiOfLength(10), 2);
        TermSuggestion termSuggestion = new TermSuggestion(randomAsciiOfLength(10), 2, SortBy.SCORE);
        suggestions = Arrays.asList(completionSuggestion, phraseSuggestion, termSuggestion);
        Suggest suggest = new Suggest(suggestions);
        List<PhraseSuggestion> phraseSuggestions = suggest.filter(PhraseSuggestion.class);
        assertThat(phraseSuggestions.size(), equalTo(1));
        assertThat(phraseSuggestions.get(0), equalTo(phraseSuggestion));
        List<TermSuggestion> termSuggestions = suggest.filter(TermSuggestion.class);
        assertThat(termSuggestions.size(), equalTo(1));
        assertThat(termSuggestions.get(0), equalTo(termSuggestion));
        List<CompletionSuggestion> completionSuggestions = suggest.filter(CompletionSuggestion.class);
        assertThat(completionSuggestions.size(), equalTo(1));
        assertThat(completionSuggestions.get(0), equalTo(completionSuggestion));
    }

    public void testSuggestionOrdering() throws Exception {
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions;
        suggestions = new ArrayList<>();
        int n = randomIntBetween(2, 5);
        for (int i = 0; i < n; i++) {
            suggestions.add(new CompletionSuggestion(randomAsciiOfLength(10), randomIntBetween(3, 5)));
        }
        Collections.shuffle(suggestions, random());
        Suggest suggest = new Suggest(suggestions);
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> sortedSuggestions;
        sortedSuggestions = new ArrayList<>(suggestions);
        sortedSuggestions.sort((o1, o2) -> o1.getName().compareTo(o2.getName()));
        List<CompletionSuggestion> completionSuggestions = suggest.filter(CompletionSuggestion.class);
        assertThat(completionSuggestions.size(), equalTo(n));
        for (int i = 0; i < n; i++) {
            assertThat(completionSuggestions.get(i).getName(), equalTo(sortedSuggestions.get(i).getName()));
        }
    }

}
