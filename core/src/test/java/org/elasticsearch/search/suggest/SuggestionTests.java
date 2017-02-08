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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class SuggestionTests extends ESTestCase {

    public static Suggestion<Entry<Option>> createTestItem() {
        String name = randomAsciiOfLengthBetween(5, 10);
        // note: size will not be rendered via "toXContent", only passed on internally on transport layer
        int size = randomInt();
        @SuppressWarnings("unchecked")
        Suggestion[] suggestions = new Suggestion[] { new TermSuggestion(name, size, randomFrom(SortBy.values())),
                new PhraseSuggestion(name, size), new CompletionSuggestion(name, size) };
        int type = randomIntBetween(0, 2);
        Suggestion suggestion = suggestions[type];
        int numEntries = randomIntBetween(0, 5);
        if (type == 2) {
            numEntries = randomIntBetween(0, 1); // CompletionSuggestion must have only one entry
        }
        for (int i = 0; i < numEntries; i++) {
            suggestion.addTerm(SuggestionEntryTests.createTestItem(type));
        }
        return suggestion;
    }

    public void testFromXContent() throws IOException {
        Suggestion<Entry<Option>> suggestion = createTestItem();
        XContentType xContentType = XContentType.JSON; //randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toXContent(suggestion, xContentType, humanReadable);
        Suggestion<Entry<Option>> parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            parsed = Suggestion.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(suggestion.getName(), parsed.getName());
        assertEquals(suggestion.getEntries().size(), parsed.getEntries().size());
        // TODO re-check whats a good default value for size here. We don't send it via xContent, so we need to pick something
        assertEquals(-1, parsed.getSize());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Option option = new Option(new Text("someText"), new Text("somethingHighlighted"), 1.3f, true);
        Entry<Option> entry = new Entry<>(new Text("entryText"), 42, 313);
        entry.addOption(option);
        BytesReference xContent = toXContent(entry, XContentType.JSON, randomBoolean());
        assertEquals(
                "{\"text\":\"entryText\","
                + "\"offset\":42,"
                + "\"length\":313,"
                + "\"options\":["
                    + "{\"text\":\"someText\","
                    + "\"highlighted\":\"somethingHighlighted\","
                    + "\"score\":1.3,"
                    + "\"collate_match\":true}"
                + "]}", xContent.utf8ToString());
    }
}
