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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitTests;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class CompletionSuggestionOptionTests extends ESTestCase {

    private static final ObjectParser<Map<String, Object>, Void> PARSER = new ObjectParser<>(
        "CompletionOptionParser",
        SearchResponseUtils.unknownMetaFieldConsumer,
        HashMap::new
    );

    static {
        SearchResponseUtils.declareInnerHitsParseFields(PARSER);
        PARSER.declareString(
            (map, value) -> map.put(Suggest.Suggestion.Entry.Option.TEXT.getPreferredName(), value),
            Suggest.Suggestion.Entry.Option.TEXT
        );
        PARSER.declareFloat(
            (map, value) -> map.put(Suggest.Suggestion.Entry.Option.SCORE.getPreferredName(), value),
            Suggest.Suggestion.Entry.Option.SCORE
        );
        PARSER.declareObject(
            (map, value) -> map.put(CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName(), value),
            (p, c) -> parseContexts(p),
            CompletionSuggestion.Entry.Option.CONTEXTS
        );
    }

    private static Map<String, Set<String>> parseContexts(XContentParser parser) throws IOException {
        Map<String, Set<String>> contexts = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String key = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            Set<String> values = new HashSet<>();
            while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser);
                values.add(parser.text());
            }
            contexts.put(key, values);
        }
        return contexts;
    }

    public static Option parseOption(XContentParser parser) {
        Map<String, Object> values = PARSER.apply(parser, null);

        Text text = new Text((String) values.get(Suggest.Suggestion.Entry.Option.TEXT.getPreferredName()));
        Float score = (Float) values.get(Suggest.Suggestion.Entry.Option.SCORE.getPreferredName());
        @SuppressWarnings("unchecked")
        Map<String, Set<String>> contexts = (Map<String, Set<String>>) values.get(
            CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName()
        );
        if (contexts == null) {
            contexts = Collections.emptyMap();
        }

        SearchHit hit = null;
        // the option either prints SCORE or inlines the search hit
        if (score == null) {
            hit = SearchResponseUtils.searchHitFromMap(values);
            score = hit.getScore();
        }
        CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(-1, text, score, contexts);
        option.setHit(hit);
        return option;
    }

    public static Option createTestItem() {
        Text text = new Text(randomAlphaOfLengthBetween(5, 15));
        int docId = randomInt();
        int numberOfContexts = randomIntBetween(0, 3);
        Map<String, Set<String>> contexts = new HashMap<>();
        for (int i = 0; i < numberOfContexts; i++) {
            int numberOfValues = randomIntBetween(0, 3);
            Set<String> values = new HashSet<>();
            for (int v = 0; v < numberOfValues; v++) {
                values.add(randomAlphaOfLengthBetween(5, 15));
            }
            contexts.put(randomAlphaOfLengthBetween(5, 15), values);
        }
        SearchHit hit = null;
        float score = randomFloat();
        if (randomBoolean()) {
            hit = SearchHitTests.createTestItem(false, true);
            score = hit.getScore();
        }
        Option option = new CompletionSuggestion.Entry.Option(docId, text, score, contexts);
        option.setHit(hit);
        if (hit != null) {
            hit.decRef();
        }
        return option;
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
            // "contexts" is an object consisting of key/array pairs, we shouldn't add anything random there
            // also there can be inner search hits fields inside this option, we need to exclude another couple of paths
            // where we cannot add random stuff. We also exclude the root level, this is done for SearchHits as all unknown fields
            // for SearchHit on a root level are interpreted as meta-fields and will be kept
            Predicate<String> excludeFilter = (path) -> path.endsWith(CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName())
                || path.endsWith("highlight")
                || path.contains("fields")
                || path.contains("_source")
                || path.contains("inner_hits")
                || path.isEmpty();
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        Option parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsed = parseOption(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(option.getText(), parsed.getText());
        assertEquals(option.getHighlighted(), parsed.getHighlighted());
        assertEquals(option.getScore(), parsed.getScore(), Float.MIN_VALUE);
        assertEquals(option.collateMatch(), parsed.collateMatch());
        assertEquals(option.getContexts(), parsed.getContexts());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Map<String, Set<String>> contexts = Collections.singletonMap("key", Collections.singleton("value"));
        CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(1, new Text("someText"), 1.3f, contexts);
        BytesReference xContent = toXContent(option, XContentType.JSON, randomBoolean());
        assertEquals("""
            {"text":"someText","score":1.3,"contexts":{"key":["value"]}}""", xContent.utf8ToString());
    }
}
