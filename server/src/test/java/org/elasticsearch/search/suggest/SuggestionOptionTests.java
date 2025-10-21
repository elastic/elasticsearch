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
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option.COLLATE_MATCH;
import static org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option.HIGHLIGHTED;
import static org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option.SCORE;
import static org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option.TEXT;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SuggestionOptionTests extends ESTestCase {

    private static final ConstructingObjectParser<PhraseSuggestion.Entry.Option, Void> PHRASE_OPTION_PARSER =
        new ConstructingObjectParser<>("PhraseOptionParser", true, args -> {
            Text text = new Text((String) args[0]);
            float score = (Float) args[1];
            String highlighted = (String) args[2];
            Text highlightedText = highlighted == null ? null : new Text(highlighted);
            Boolean collateMatch = (Boolean) args[3];
            return new PhraseSuggestion.Entry.Option(text, highlightedText, score, collateMatch);
        });

    static {
        PHRASE_OPTION_PARSER.declareString(constructorArg(), TEXT);
        PHRASE_OPTION_PARSER.declareFloat(constructorArg(), SCORE);
        PHRASE_OPTION_PARSER.declareString(optionalConstructorArg(), HIGHLIGHTED);
        PHRASE_OPTION_PARSER.declareBoolean(optionalConstructorArg(), COLLATE_MATCH);
    }

    public static PhraseSuggestion.Entry.Option parsePhraseSuggestionOption(XContentParser parser) {
        return PHRASE_OPTION_PARSER.apply(parser, null);
    }

    public static Option createTestItem() {
        Text text = new Text(randomAlphaOfLengthBetween(5, 15));
        float score = randomFloat();
        Text highlighted = randomFrom((Text) null, new Text(randomAlphaOfLengthBetween(5, 15)));
        Boolean collateMatch = randomFrom((Boolean) null, randomBoolean());
        return new PhraseSuggestion.Entry.Option(text, highlighted, score, collateMatch);
    }

    public void testFromXContent() throws IOException {
        doTestFromXContent(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doTestFromXContent(true);
    }

    private void doTestFromXContent(boolean addRandomFields) throws IOException {
        Option option = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(option, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xContentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        Option parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = parsePhraseSuggestionOption(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals(option.getText(), parsed.getText());
        assertEquals(option.getHighlighted(), parsed.getHighlighted());
        assertEquals(option.getScore(), parsed.getScore(), Float.MIN_VALUE);
        assertEquals(option.collateMatch(), parsed.collateMatch());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Option option = new PhraseSuggestion.Entry.Option(new Text("someText"), new Text("somethingHighlighted"), 1.3f, true);
        BytesReference xContent = toXContent(option, XContentType.JSON, randomBoolean());
        assertEquals(("""
            {
              "text": "someText",
              "highlighted": "somethingHighlighted",
              "score": 1.3,
              "collate_match": true
            }""").replaceAll("\\s+", ""), xContent.utf8ToString());
    }
}
