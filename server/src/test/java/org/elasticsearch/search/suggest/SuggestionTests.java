/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.suggest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static final Class<Suggestion<? extends Entry<? extends Option>>>[] SUGGESTION_TYPES = new Class[] {
        TermSuggestion.class,
        PhraseSuggestion.class,
        CompletionSuggestion.class };

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
                // We also exclude options that contain SearchHits, as all unknown fields
                // on a root level of SearchHit are interpreted as meta-fields and will be kept.
                Predicate<String> excludeFilter = path -> path.isEmpty()
                    || path.endsWith(CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName())
                    || path.endsWith("highlight")
                    || path.contains("fields")
                    || path.contains("_source")
                    || path.contains("inner_hits")
                    || path.contains("options");
                mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
            } else {
                mutated = originalBytes;
            }
            Suggestion parsed;
            try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
                parsed = SearchResponseUtils.parseSuggestion(parser);
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
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            assertNull(SearchResponseUtils.parseSuggestion(parser));
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        }
    }

    public void testUnknownSuggestionTypeThrows() throws IOException {
        XContent xContent = JsonXContent.jsonXContent;
        String suggestionString = ("""
            {
              "unknownType#suggestionName": [
                {
                  "text": "entryText",
                  "offset": 42,
                  "length": 313,
                  "options": [
                    {
                      "text": "someText",
                      "highlighted": "somethingHighlighted",
                      "score": 1.3,
                      "collate_match": true
                    }
                  ]
                }
              ]
            }""").replaceAll("\\s+", "");
        try (
            XContentParser parser = xContent.createParser(
                XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()),
                suggestionString
            )
        ) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            NamedObjectNotFoundException e = expectThrows(
                NamedObjectNotFoundException.class,
                () -> SearchResponseUtils.parseSuggestion(parser)
            );
            assertEquals("[1:31] unknown field [unknownType]", e.getMessage());
        }
    }

    public void testToXContent() throws IOException {
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        {
            PhraseSuggestion.Entry.Option option = new PhraseSuggestion.Entry.Option(
                new Text("someText"),
                new Text("somethingHighlighted"),
                1.3f,
                true
            );
            PhraseSuggestion.Entry entry = new PhraseSuggestion.Entry(new Text("entryText"), 42, 313);
            entry.addOption(option);
            PhraseSuggestion suggestion = new PhraseSuggestion("suggestionName", 5);
            suggestion.addTerm(entry);
            BytesReference xContent = toXContent(suggestion, XContentType.JSON, params, randomBoolean());
            assertEquals("""
                {
                  "phrase#suggestionName": [
                    {
                      "text": "entryText",
                      "offset": 42,
                      "length": 313,
                      "options": [
                        {
                          "text": "someText",
                          "highlighted": "somethingHighlighted",
                          "score": 1.3,
                          "collate_match": true
                        }
                      ]
                    }
                  ]
                }""".replaceAll("\\s+", ""), xContent.utf8ToString());
        }
        {
            PhraseSuggestion.Entry.Option option = new PhraseSuggestion.Entry.Option(
                new Text("someText"),
                new Text("somethingHighlighted"),
                1.3f,
                true
            );
            PhraseSuggestion.Entry entry = new PhraseSuggestion.Entry(new Text("entryText"), 42, 313, 1.0);
            entry.addOption(option);
            PhraseSuggestion suggestion = new PhraseSuggestion("suggestionName", 5);
            suggestion.addTerm(entry);
            BytesReference xContent = toXContent(suggestion, XContentType.JSON, params, randomBoolean());
            assertEquals("""
                {
                  "phrase#suggestionName": [
                    {
                      "text": "entryText",
                      "offset": 42,
                      "length": 313,
                      "options": [
                        {
                          "text": "someText",
                          "highlighted": "somethingHighlighted",
                          "score": 1.3,
                          "collate_match": true
                        }
                      ]
                    }
                  ]
                }""".replaceAll("\\s+", ""), xContent.utf8ToString());
        }
        {
            TermSuggestion.Entry.Option option = new TermSuggestion.Entry.Option(new Text("someText"), 10, 1.3f);
            TermSuggestion.Entry entry = new TermSuggestion.Entry(new Text("entryText"), 42, 313);
            entry.addOption(option);
            TermSuggestion suggestion = new TermSuggestion("suggestionName", 5, SortBy.SCORE);
            suggestion.addTerm(entry);
            BytesReference xContent = toXContent(suggestion, XContentType.JSON, params, randomBoolean());
            assertEquals("""
                {
                  "term#suggestionName": [
                    {
                      "text": "entryText",
                      "offset": 42,
                      "length": 313,
                      "options": [ { "text": "someText", "score": 1.3, "freq": 10 } ]
                    }
                  ]
                }""".replaceAll("\\s+", ""), xContent.utf8ToString());
        }
        {
            Map<String, Set<String>> contexts = Collections.singletonMap("key", Collections.singleton("value"));
            CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(1, new Text("someText"), 1.3f, contexts);
            CompletionSuggestion.Entry entry = new CompletionSuggestion.Entry(new Text("entryText"), 42, 313);
            entry.addOption(option);
            CompletionSuggestion suggestion = new CompletionSuggestion("suggestionName", 5, randomBoolean());
            suggestion.addTerm(entry);
            BytesReference xContent = toXContent(suggestion, XContentType.JSON, params, randomBoolean());
            assertEquals("""
                {
                  "completion#suggestionName": [
                    {
                      "text": "entryText",
                      "offset": 42,
                      "length": 313,
                      "options": [
                        {
                          "text": "someText",
                          "score": 1.3,
                          "contexts": {
                            "key": [ "value" ]
                          }
                        }
                      ]
                    }
                  ]
                }""".replaceAll("\\s+", ""), xContent.utf8ToString());
        }
    }
}
