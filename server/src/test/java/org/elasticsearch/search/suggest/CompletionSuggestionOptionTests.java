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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitTests;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class CompletionSuggestionOptionTests extends ESTestCase {

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
                    || path.endsWith("highlight") || path.contains("fields") || path.contains("_source") || path.contains("inner_hits")
                    || path.isEmpty();
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        Option parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsed = Option.fromXContent(parser);
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
        assertEquals("{\"text\":\"someText\",\"score\":1.3,\"contexts\":{\"key\":[\"value\"]}}"
                   , xContent.utf8ToString());
    }
}
