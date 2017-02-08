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
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion.Entry.Option;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class TermSuggestionOptionTests extends ESTestCase {

    public static TermSuggestion.Entry.Option createTestItem() {
        Text text = new Text(randomAsciiOfLengthBetween(5, 10));
        float score = randomFloat();
        int freq = randomInt(1000);
        return new TermSuggestion.Entry.Option(text, freq, score);
    }

    public void testFromXContent() throws IOException {
        TermSuggestion.Entry.Option option = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toXContent(option, xContentType, humanReadable);
        org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            parsed = org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertThat(parsed, instanceOf(TermSuggestion.Entry.Option.class));
        TermSuggestion.Entry.Option parsedTermSuggestionOption = (TermSuggestion.Entry.Option) parsed;
        assertEquals(option.getText(), parsedTermSuggestionOption.getText());
        assertEquals(option.getHighlighted(), parsedTermSuggestionOption.getHighlighted());
        assertEquals(option.getScore(), parsedTermSuggestionOption.getScore(), Float.MIN_VALUE);
        assertEquals(option.collateMatch(), parsedTermSuggestionOption.collateMatch());
        assertEquals(option.getFreq(), parsedTermSuggestionOption.getFreq());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Option option = new Option(new Text("someText"), 100, 1.3f);
        BytesReference xContent = toXContent(option, XContentType.JSON, randomBoolean());
        assertEquals("{\"text\":\"someText\",\"score\":1.3,\"freq\":100}"
                   , xContent.utf8ToString());


    }

}
