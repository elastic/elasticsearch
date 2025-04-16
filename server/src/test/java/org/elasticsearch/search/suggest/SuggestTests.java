/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.suggest;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;

public class SuggestTests extends ESTestCase {

    private static final NamedXContentRegistry xContentRegistry;
    private static final List<NamedXContentRegistry.Entry> namedXContents;

    static {
        namedXContents = new ArrayList<>();
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                Suggest.Suggestion.class,
                new ParseField("term"),
                (parser, context) -> parseTermSuggestion(parser, (String) context)
            )
        );
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                Suggest.Suggestion.class,
                new ParseField("phrase"),
                (parser, context) -> parsePhraseSuggestion(parser, (String) context)
            )
        );
        namedXContents.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField("completion"), (parser, context) -> {
            CompletionSuggestion suggestion = new CompletionSuggestion((String) context, -1, false);
            parseEntries(parser, suggestion, SuggestionEntryTests::parseCompletionSuggestionEntry);
            return suggestion;
        }));
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    public static PhraseSuggestion parsePhraseSuggestion(XContentParser parser, String name) throws IOException {
        PhraseSuggestion suggestion = new PhraseSuggestion(name, -1);
        parseEntries(parser, suggestion, SuggestionEntryTests::parsePhraseSuggestionEntry);
        return suggestion;
    }

    private static <E extends Suggestion.Entry<?>> void parseEntries(
        XContentParser parser,
        Suggestion<E> suggestion,
        CheckedFunction<XContentParser, E, IOException> entryParser
    ) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            suggestion.addTerm(entryParser.apply(parser));
        }
    }

    static void declareCommonEntryParserFields(ObjectParser<? extends Entry<? extends Option>, Void> parser) {
        parser.declareString((entry, text) -> entry.text = new Text(text), new ParseField(Suggestion.Entry.TEXT));
        parser.declareInt((entry, offset) -> entry.offset = offset, new ParseField(Suggestion.Entry.OFFSET));
        parser.declareInt((entry, length) -> entry.length = length, new ParseField(Suggestion.Entry.LENGTH));
    }

    public static TermSuggestion parseTermSuggestion(XContentParser parser, String name) throws IOException {
        // the "size" parameter and the SortBy for TermSuggestion cannot be parsed from the response, use default values
        TermSuggestion suggestion = new TermSuggestion(name, -1, SortBy.SCORE);
        parseEntries(parser, suggestion, SuggestTests::parseTermSuggestionEntry);
        return suggestion;
    }

    private static final ObjectParser<TermSuggestion.Entry, Void> PARSER = new ObjectParser<>(
        "TermSuggestionEntryParser",
        true,
        TermSuggestion.Entry::new
    );
    static {
        declareCommonEntryParserFields(PARSER);
        /*
         * The use of a lambda expression instead of the method reference Entry::addOptions is a workaround for a JDK 14 compiler bug.
         * The bug is: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8242214
         */
        PARSER.declareObjectArray(
            (e, o) -> e.addOptions(o),
            (p, c) -> TermSuggestionOptionTests.parseEntryOption(p),
            new ParseField(Suggest.Suggestion.Entry.OPTIONS)
        );
    }

    public static TermSuggestion.Entry parseTermSuggestionEntry(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        return namedXContents;
    }

    static NamedXContentRegistry getSuggestersRegistry() {
        return xContentRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return getSuggestersRegistry();
    }

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
        BytesReference originalBytes = toShuffledXContent(suggest, xContentType, params, humanReadable);
        Suggest parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), Suggest.NAME);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = SearchResponseUtils.parseSuggest(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(suggest.size(), parsed.size());
        for (Suggestion<?> suggestion : suggest) {
            Suggestion<? extends Entry<? extends Option>> parsedSuggestion = parsed.getSuggestion(suggestion.getName());
            assertNotNull(parsedSuggestion);
            assertEquals(suggestion.getClass(), parsedSuggestion.getClass());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, params, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
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
        Suggest suggest = new Suggest(Collections.singletonList(suggestion));
        BytesReference xContent = toXContent(suggest, XContentType.JSON, randomBoolean());
        assertEquals(stripWhitespace("""
            {
              "suggest": {
                "suggestionName": [
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
              }
            }"""), xContent.utf8ToString());
    }

    public void testFilter() throws Exception {
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions;
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(randomAlphaOfLength(10), 2, false);
        PhraseSuggestion phraseSuggestion = new PhraseSuggestion(randomAlphaOfLength(10), 2);
        TermSuggestion termSuggestion = new TermSuggestion(randomAlphaOfLength(10), 2, SortBy.SCORE);
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
            suggestions.add(new CompletionSuggestion(randomAlphaOfLength(10), randomIntBetween(3, 5), false));
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

    public void testParsingExceptionOnUnknownSuggestion() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startArray("unknownSuggestion");
            builder.endArray();
        }
        builder.endObject();
        BytesReference originalBytes = BytesReference.bytes(builder);
        try (XContentParser parser = createParser(builder.contentType().xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            ParsingException ex = expectThrows(ParsingException.class, () -> SearchResponseUtils.parseSuggest(parser));
            assertEquals("Could not parse suggestion keyed as [unknownSuggestion]", ex.getMessage());
        }
    }

    public void testMergingSuggestionOptions() {
        String suggestedWord = randomAlphaOfLength(10);
        String secondWord = randomAlphaOfLength(10);
        Text suggestionText = new Text(suggestedWord + " " + secondWord);
        Text highlighted = new Text("<em>" + suggestedWord + "</em> " + secondWord);
        PhraseSuggestion.Entry.Option option1 = new PhraseSuggestion.Entry.Option(suggestionText, highlighted, 0.7f, false);
        PhraseSuggestion.Entry.Option option2 = new PhraseSuggestion.Entry.Option(suggestionText, highlighted, 0.8f, true);
        PhraseSuggestion.Entry.Option option3 = new PhraseSuggestion.Entry.Option(suggestionText, highlighted, 0.6f);
        assertEquals(suggestionText, option1.getText());
        assertEquals(highlighted, option1.getHighlighted());
        assertFalse(option1.collateMatch());
        assertTrue(option1.getScore() > 0.6f);
        option1.mergeInto(option2);
        assertEquals(suggestionText, option1.getText());
        assertEquals(highlighted, option1.getHighlighted());
        assertTrue(option1.collateMatch());
        assertTrue(option1.getScore() > 0.7f);
        option1.mergeInto(option3);
        assertEquals(suggestionText, option1.getText());
        assertEquals(highlighted, option1.getHighlighted());
        assertTrue(option1.getScore() > 0.7f);
        assertTrue(option1.collateMatch());
    }

    public void testSerialization() throws IOException {
        TransportVersion bwcVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.MINIMUM_COMPATIBLE,
            TransportVersion.current()
        );

        final Suggest suggest = createTestItem();
        // suggest is disallowed when using rank, but the randomization rarely sets it
        // we need to make sure CompletionSuggestion$Entry$Option doesn't have "rank" set
        // because for some older versions it will not serialize.
        if (bwcVersion.before(TransportVersions.V_8_8_0)) {
            for (CompletionSuggestion s : suggest.filter(CompletionSuggestion.class)) {
                for (CompletionSuggestion.Entry entry : s.entries) {
                    List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                    for (CompletionSuggestion.Entry.Option o : entry.getOptions()) {
                        if (o.getHit() != null) {
                            o.getHit().setRank(-1);
                        }
                    }
                }
            }
        }

        final Suggest bwcSuggest;

        NamedWriteableRegistry registry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(bwcVersion);
            suggest.writeTo(out);
            try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                in.setTransportVersion(bwcVersion);
                bwcSuggest = new Suggest(in);
            }
        }

        assertEquals(suggest, bwcSuggest);

        final Suggest backAgain;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            bwcSuggest.writeTo(out);
            try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                in.setTransportVersion(TransportVersion.current());
                backAgain = new Suggest(in);
            }
        }

        assertEquals(suggest, backAgain);
    }
}
