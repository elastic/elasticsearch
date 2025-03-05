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
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion.Entry.Option;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class TermSuggestionOptionTests extends ESTestCase {

    public static Option createTestItem() {
        Text text = new Text(randomAlphaOfLengthBetween(5, 15));
        float score = randomFloat();
        int freq = randomInt();
        return new Option(text, freq, score);
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
            parsed = parseEntryOption(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals(option.getText(), parsed.getText());
        assertEquals(option.getScore(), parsed.getScore(), Float.MIN_VALUE);
        assertEquals(option.getFreq(), parsed.getFreq());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Option option = new Option(new Text("someText"), 100, 1.3f);
        BytesReference xContent = toXContent(option, XContentType.JSON, randomBoolean());
        assertEquals("""
            {"text":"someText","score":1.3,"freq":100}""", xContent.utf8ToString());
    }

    private static final ConstructingObjectParser<Option, Void> PARSER = new ConstructingObjectParser<>(
        "TermSuggestionOptionParser",
        true,
        args -> {
            Text text = new Text((String) args[0]);
            int freq = (Integer) args[1];
            float score = (Float) args[2];
            return new Option(text, freq, score);
        }
    );

    static {
        PARSER.declareString(constructorArg(), Suggest.Suggestion.Entry.Option.TEXT);
        PARSER.declareInt(constructorArg(), TermSuggestion.Entry.Option.FREQ);
        PARSER.declareFloat(constructorArg(), Suggest.Suggestion.Entry.Option.SCORE);
    }

    public static Option parseEntryOption(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
