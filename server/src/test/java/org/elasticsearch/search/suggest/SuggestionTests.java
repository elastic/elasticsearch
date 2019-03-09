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
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class SuggestionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    private static final Class<Suggestion<? extends Entry<? extends Option>>>[] SUGGESTION_TYPES = new Class[] {
        TermSuggestion.class, PhraseSuggestion.class, CompletionSuggestion.class
    };

    public static Suggestion<? extends Entry<? extends Option>> createTestItem() {
        return createTestItem(randomFrom(SUGGESTION_TYPES));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return SuggestTests.getSuggestersRegistry();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Suggestion<? extends Entry<? extends Option>> createTestItem(Class<? extends Suggestion> type) {
        String name = randomAlphaOfLengthBetween(5, 10);
        // note: size will not be rendered via "toXContent", only passed on internally on transport layer
        int size = randomInt();
        Supplier<Entry> entrySupplier;
        Suggestion suggestion;
        if (type == TermSuggestion.class) {
            suggestion = new TermSuggestion(name, size, randomFrom(SortBy.values()));
            entrySupplier = () -> SuggestionEntryTests.createTestItem(TermSuggestion.Entry.class);
        } else if (type == PhraseSuggestion.class) {
            suggestion = new PhraseSuggestion(name, size);
            entrySupplier = () -> SuggestionEntryTests.createTestItem(PhraseSuggestion.Entry.class);
        } else if (type == CompletionSuggestion.class) {
            suggestion = new CompletionSuggestion(name, size, randomBoolean());
            entrySupplier = () -> SuggestionEntryTests.createTestItem(CompletionSuggestion.Entry.class);
        } else {
            throw new UnsupportedOperationException("type not supported [" + type + "]");
        }
        int numEntries;
        if (frequently()) {
            if (type == CompletionSuggestion.class) {
                numEntries = 1; // CompletionSuggestion can have max. one entry
            } else {
                numEntries = randomIntBetween(1, 5);
            }
        } else {
            numEntries = 0; // also occasionally test zero entries
        }
        for (int i = 0; i < numEntries; i++) {
            suggestion.addTerm(entrySupplier.get());
        }
        return suggestion;
    }

    public void testFromXContent() throws IOException {
        doTestFromXContent(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doTestFromXContent(true);
    }

    @SuppressWarnings({ "rawtypes" })
    private void doTestFromXContent(boolean addRandomFields) throws IOException {
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        for (Class<Suggestion<? extends Entry<? extends Option>>> type : SUGGESTION_TYPES) {
            Suggestion suggestion = createTestItem(type);
            XContentType xContentType = randomFrom(XContentType.values());
            boolean humanReadable = randomBoolean();
            BytesReference originalBytes = toShuffledXContent(suggestion, xContentType, params, humanReadable);
            BytesReference mutated;
            if (addRandomFields) {
                // - "contexts" is an object consisting of key/array pairs, we shouldn't add anything random there
                // - there can be inner search hits fields inside this option where we cannot add random stuff
                // - the root object should be excluded since it contains the named suggestion arrays
                Predicate<String> excludeFilter = path -> (path.isEmpty()
                        || path.endsWith(CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName()) || path.endsWith("highlight")
                        || path.endsWith("fields") || path.contains("_source") || path.contains("inner_hits"));
                mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
            } else {
                mutated = originalBytes;
            }
            Suggestion parsed;
            try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
                parsed = Suggestion.fromXContent(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                assertNull(parser.nextToken());
            }
            assertEquals(suggestion.getName(), parsed.getName());
            assertEquals(suggestion.getEntries().size(), parsed.getEntries().size());
            // We don't parse size via xContent, instead we set it to -1 on the client side
            assertEquals(-1, parsed.getSize());
            assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, params, humanReadable), xContentType);
        }
    }

    /**
     * test that we parse nothing if RestSearchAction.TYPED_KEYS_PARAM isn't set while rendering xContent and we cannot find
     * suggestion type information
     */
    public void testFromXContentWithoutTypeParam() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toXContent(createTestItem(), xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
            assertNull(Suggestion.fromXContent(parser));
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        }
    }

    public void testUnknownSuggestionTypeThrows() throws IOException {
        XContent xContent = JsonXContent.jsonXContent;
        String suggestionString =
                 "{\"unknownType#suggestionName\":"
                    + "[{\"text\":\"entryText\","
                    + "\"offset\":42,"
                    + "\"length\":313,"
                    + "\"options\":[{\"text\":\"someText\","
                                + "\"highlighted\":\"somethingHighlighted\","
                                + "\"score\":1.3,"
                                + "\"collate_match\":true}]"
                            + "}]"
                + "}";
        try (XContentParser parser = xContent.createParser(xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, suggestionString)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
            NamedObjectNotFoundException e = expectThrows(NamedObjectNotFoundException.class, () -> Suggestion.fromXContent(parser));
            assertEquals("[1:31] unable to parse Suggestion with name [unknownType]: parser not found", e.getMessage());
        }
    }

    public void testToXContent() throws IOException {
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        {
            PhraseSuggestion.Entry.Option option = new PhraseSuggestion.Entry.Option(new Text("someText"), new Text("somethingHighlighted"),
                1.3f, true);
            PhraseSuggestion.Entry entry = new PhraseSuggestion.Entry(new Text("entryText"), 42, 313);
            entry.addOption(option);
            PhraseSuggestion suggestion = new PhraseSuggestion("suggestionName", 5);
            suggestion.addTerm(entry);
            BytesReference xContent = toXContent(suggestion, XContentType.JSON, params, randomBoolean());
            assertEquals(
                    "{\"phrase#suggestionName\":[{"
                            + "\"text\":\"entryText\","
                            + "\"offset\":42,"
                            + "\"length\":313,"
                            + "\"options\":[{"
                                + "\"text\":\"someText\","
                                + "\"highlighted\":\"somethingHighlighted\","
                                + "\"score\":1.3,"
                                + "\"collate_match\":true}]"
                            + "}]"
                    + "}", xContent.utf8ToString());
        }
        {
            PhraseSuggestion.Entry.Option option = new PhraseSuggestion.Entry.Option(new Text("someText"), new Text("somethingHighlighted"),
                    1.3f, true);
            PhraseSuggestion.Entry entry = new PhraseSuggestion.Entry(new Text("entryText"), 42, 313, 1.0);
            entry.addOption(option);
            PhraseSuggestion suggestion = new PhraseSuggestion("suggestionName", 5);
            suggestion.addTerm(entry);
            BytesReference xContent = toXContent(suggestion, XContentType.JSON, params, randomBoolean());
            assertEquals(
                    "{\"phrase#suggestionName\":[{"
                            + "\"text\":\"entryText\","
                            + "\"offset\":42,"
                            + "\"length\":313,"
                            + "\"options\":[{"
                                + "\"text\":\"someText\","
                                + "\"highlighted\":\"somethingHighlighted\","
                                + "\"score\":1.3,"
                                + "\"collate_match\":true}]"
                            + "}]"
                    + "}", xContent.utf8ToString());
        }
        {
            TermSuggestion.Entry.Option option = new TermSuggestion.Entry.Option(new Text("someText"), 10, 1.3f);
            TermSuggestion.Entry entry = new TermSuggestion.Entry(new Text("entryText"), 42, 313);
            entry.addOption(option);
            TermSuggestion suggestion = new TermSuggestion("suggestionName", 5, SortBy.SCORE);
            suggestion.addTerm(entry);
            BytesReference xContent = toXContent(suggestion, XContentType.JSON, params, randomBoolean());
            assertEquals(
                    "{\"term#suggestionName\":[{"
                        + "\"text\":\"entryText\","
                        + "\"offset\":42,"
                        + "\"length\":313,"
                        + "\"options\":[{"
                            + "\"text\":\"someText\","
                            + "\"score\":1.3,"
                            + "\"freq\":10}]"
                        + "}]"
                    + "}", xContent.utf8ToString());
        }
        {
            Map<String, Set<String>> contexts = Collections.singletonMap("key", Collections.singleton("value"));
            CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(1, new Text("someText"), 1.3f, contexts);
            CompletionSuggestion.Entry entry = new CompletionSuggestion.Entry(new Text("entryText"), 42, 313);
            entry.addOption(option);
            CompletionSuggestion suggestion = new CompletionSuggestion("suggestionName", 5, randomBoolean());
            suggestion.addTerm(entry);
            BytesReference xContent = toXContent(suggestion, XContentType.JSON, params, randomBoolean());
            assertEquals(
                    "{\"completion#suggestionName\":[{"
                        + "\"text\":\"entryText\","
                        + "\"offset\":42,"
                        + "\"length\":313,"
                        + "\"options\":[{"
                            + "\"text\":\"someText\","
                            + "\"score\":1.3,"
                            + "\"contexts\":{\"key\":[\"value\"]}"
                        + "}]"
                    + "}]}", xContent.utf8ToString());
        }
    }
}
