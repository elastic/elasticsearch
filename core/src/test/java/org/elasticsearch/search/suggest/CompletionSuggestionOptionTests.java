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
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitTests;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class CompletionSuggestionOptionTests extends ESTestCase {

    public static CompletionSuggestion.Entry.Option createTestItem() {
        Text text = new Text(randomAsciiOfLengthBetween(5, 10));
        int docId = randomInt(1000);
        int numberOfContexts = randomIntBetween(0, 5);
        Map<String, Set<CharSequence>> contexts = new HashMap<>();
        for (int i = 0; i < numberOfContexts; i++) {
            int numberOfValues = randomIntBetween(0, 5);
            Set<CharSequence> values = new HashSet<>();
            for (int v = 0; v < numberOfValues; v++) {
                values.add(randomAsciiOfLengthBetween(4, 10));
            }
            contexts.put(randomAsciiOfLength(5), values);
        }
        // TODO re-check with Areek, we should always have a hit set to the CompletionSuggestion.Entry.Option
        InternalSearchHit hit = InternalSearchHitTests.createTestItem(false);
        // we use the hit.score() here because it should always be the same
        CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(docId, text, hit.score(), contexts);
        option.setHit(hit);
        return option;
    }

    public void testFromXContent() throws IOException {
        CompletionSuggestion.Entry.Option option = createTestItem();
        XContentType xContentType = XContentType.JSON; //randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toXContent(option, xContentType, humanReadable);
        Option parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            parsed = Option.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertThat(parsed, instanceOf(CompletionSuggestion.Entry.Option.class));
        CompletionSuggestion.Entry.Option parsedOption = (CompletionSuggestion.Entry.Option) parsed;
        assertEquals(option.getText(), parsedOption.getText());
        assertEquals(option.getHighlighted(), parsedOption.getHighlighted());
        assertEquals(option.getScore(), parsedOption.getScore(), Float.MIN_VALUE);
        assertEquals(option.collateMatch(), parsedOption.collateMatch());
        assertEquals(option.getContexts(), parsedOption.getContexts());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Map<String, Set<CharSequence>> contexts = Collections.singletonMap("key", Collections.singleton("value"));
        CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(1, new Text("someText"), 1.3f, contexts);
        BytesReference xContent = toXContent(option, XContentType.JSON, randomBoolean());
        assertEquals("{\"text\":\"someText\",\"score\":1.3,\"contexts\":{\"key\":[\"value\"]}}"
                   , xContent.utf8ToString());
    }
}
