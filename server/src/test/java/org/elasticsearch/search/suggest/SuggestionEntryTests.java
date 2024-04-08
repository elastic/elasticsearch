/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.core.Types.forciblyCast;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class SuggestionEntryTests extends ESTestCase {

    private static final Map<Class<? extends Entry<?>>, Function<XContentParser, ? extends Entry<?>>> ENTRY_PARSERS = new HashMap<>();
    static {
        ENTRY_PARSERS.put(TermSuggestion.Entry.class, SuggestTests::parseTermSuggestionEntry);
        ENTRY_PARSERS.put(PhraseSuggestion.Entry.class, SuggestionEntryTests::parsePhraseSuggestionEntry);
        ENTRY_PARSERS.put(CompletionSuggestion.Entry.class, SuggestionEntryTests::parseCompletionSuggestionEntry);
    }

    private static final ObjectParser<PhraseSuggestion.Entry, Void> PHRASE_SUGGESTION_ENTRY_PARSER = new ObjectParser<>(
        "PhraseSuggestionEntryParser",
        true,
        PhraseSuggestion.Entry::new
    );
    static {
        SuggestTests.declareCommonEntryParserFields(PHRASE_SUGGESTION_ENTRY_PARSER);
        /*
         * The use of a lambda expression instead of the method reference Entry::addOptions is a workaround for a JDK 14 compiler bug.
         * The bug is: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8242214
         */
        PHRASE_SUGGESTION_ENTRY_PARSER.declareObjectArray(
            (e, o) -> e.addOptions(o),
            (p, c) -> SuggestionOptionTests.parsePhraseSuggestionOption(p),
            new ParseField(Entry.OPTIONS)
        );
    }

    public static PhraseSuggestion.Entry parsePhraseSuggestionEntry(XContentParser parser) {
        return PHRASE_SUGGESTION_ENTRY_PARSER.apply(parser, null);
    }

    private static final ObjectParser<CompletionSuggestion.Entry, Void> COMPLETION_SUGGESTION_ENTRY_PARSER = new ObjectParser<>(
        "CompletionSuggestionEntryParser",
        true,
        CompletionSuggestion.Entry::new
    );
    static {
        SuggestTests.declareCommonEntryParserFields(COMPLETION_SUGGESTION_ENTRY_PARSER);
        /*
         * The use of a lambda expression instead of the method reference Entry::addOptions is a workaround for a JDK 14 compiler bug.
         * The bug is: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8242214
         */
        COMPLETION_SUGGESTION_ENTRY_PARSER.declareObjectArray(
            (e, o) -> e.addOptions(o),
            (p, c) -> CompletionSuggestionOptionTests.parseOption(p),
            new ParseField(Entry.OPTIONS)
        );
    }

    public static CompletionSuggestion.Entry parseCompletionSuggestionEntry(XContentParser parser) {
        return COMPLETION_SUGGESTION_ENTRY_PARSER.apply(parser, null);
    }

    /**
     * Create a randomized Suggestion.Entry
     */
    @SuppressWarnings("unchecked")
    public static <O extends Option> Entry<O> createTestItem(Class<? extends Entry<O>> entryType) {
        Text entryText = new Text(randomAlphaOfLengthBetween(5, 15));
        int offset = randomInt();
        int length = randomInt();
        Entry<O> entry;
        Supplier<Option> supplier;
        if (entryType == (Class<? extends Entry<? extends Option>>) TermSuggestion.Entry.class) {
            entry = (Entry<O>) new TermSuggestion.Entry(entryText, offset, length);
            supplier = TermSuggestionOptionTests::createTestItem;
        } else if (entryType == (Class<? extends Entry<? extends Option>>) PhraseSuggestion.Entry.class) {
            entry = (Entry<O>) new PhraseSuggestion.Entry(entryText, offset, length, randomDouble());
            supplier = SuggestionOptionTests::createTestItem;
        } else if (entryType == (Class<? extends Entry<? extends Option>>) CompletionSuggestion.Entry.class) {
            entry = (Entry<O>) new CompletionSuggestion.Entry(entryText, offset, length);
            supplier = CompletionSuggestionOptionTests::createTestItem;
        } else {
            throw new UnsupportedOperationException("entryType not supported [" + entryType + "]");
        }
        int numOptions = randomIntBetween(0, 5);
        for (int i = 0; i < numOptions; i++) {
            entry.addOption((O) supplier.get());
        }
        return entry;
    }

    public void testFromXContent() throws IOException {
        doTestFromXContent(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doTestFromXContent(true);
    }

    @SuppressWarnings("unchecked")
    private void doTestFromXContent(boolean addRandomFields) throws IOException {
        for (Class<? extends Entry<?>> entryType : ENTRY_PARSERS.keySet()) {
            Entry<Option> entry = createTestItem((forciblyCast(entryType)));
            XContentType xContentType = randomFrom(XContentType.values());
            boolean humanReadable = randomBoolean();
            BytesReference originalBytes = toShuffledXContent(entry, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
            BytesReference mutated;
            if (addRandomFields) {
                // "contexts" is an object consisting of key/array pairs, we shouldn't add anything random there
                // also there can be inner search hits fields inside this option, we need to exclude another couple of paths
                // where we cannot add random stuff
                // exclude "options" which contain SearchHits,
                // on root level of SearchHit fields are interpreted as meta-fields and will be kept
                Predicate<String> excludeFilter = (path -> path.endsWith(CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName())
                    || path.endsWith("highlight")
                    || path.contains("fields")
                    || path.contains("_source")
                    || path.contains("inner_hits")
                    || path.contains("options"));

                mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
            } else {
                mutated = originalBytes;
            }
            Entry<Option> parsed;
            try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                parsed = (Entry<Option>) ENTRY_PARSERS.get(entry.getClass()).apply(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            assertEquals(entry.getClass(), parsed.getClass());
            assertEquals(entry.getText(), parsed.getText());
            assertEquals(entry.getLength(), parsed.getLength());
            assertEquals(entry.getOffset(), parsed.getOffset());
            assertEquals(entry.getOptions().size(), parsed.getOptions().size());
            for (int i = 0; i < entry.getOptions().size(); i++) {
                assertEquals(entry.getOptions().get(i).getClass(), parsed.getOptions().get(i).getClass());
            }
            assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
        }
    }

    public void testToXContent() throws IOException {
        PhraseSuggestion.Entry.Option phraseOption = new PhraseSuggestion.Entry.Option(
            new Text("someText"),
            new Text("somethingHighlighted"),
            1.3f,
            true
        );
        PhraseSuggestion.Entry phraseEntry = new PhraseSuggestion.Entry(new Text("entryText"), 42, 313);
        phraseEntry.addOption(phraseOption);
        BytesReference xContent = toXContent(phraseEntry, XContentType.JSON, randomBoolean());
        assertEquals(XContentHelper.stripWhitespace("""
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
            }"""), xContent.utf8ToString());

        TermSuggestion.Entry.Option termOption = new TermSuggestion.Entry.Option(new Text("termSuggestOption"), 42, 3.13f);
        TermSuggestion.Entry termEntry = new TermSuggestion.Entry(new Text("entryText"), 42, 313);
        termEntry.addOption(termOption);
        xContent = toXContent(termEntry, XContentType.JSON, randomBoolean());
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "text": "entryText",
              "offset": 42,
              "length": 313,
              "options": [ { "text": "termSuggestOption", "score": 3.13, "freq": 42 } ]
            }"""), xContent.utf8ToString());

        CompletionSuggestion.Entry.Option completionOption = new CompletionSuggestion.Entry.Option(
            -1,
            new Text("completionOption"),
            3.13f,
            Collections.singletonMap("key", Collections.singleton("value"))
        );
        CompletionSuggestion.Entry completionEntry = new CompletionSuggestion.Entry(new Text("entryText"), 42, 313);
        completionEntry.addOption(completionOption);
        xContent = toXContent(completionEntry, XContentType.JSON, randomBoolean());
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "text": "entryText",
              "offset": 42,
              "length": 313,
              "options": [
                {
                  "text": "completionOption",
                  "score": 3.13,
                  "contexts": {
                    "key": [ "value" ]
                  }
                }
              ]
            }"""), xContent.utf8ToString());
    }

}
