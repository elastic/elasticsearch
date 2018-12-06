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

package org.elasticsearch.search;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractStreamableTestCase;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class SearchHitsTests extends AbstractStreamableTestCase<SearchHits> {
    public static SearchHits createTestItem() {
        int searchHits = randomIntBetween(0, 5);
        SearchHit[] hits = new SearchHit[searchHits];
        for (int i = 0; i < searchHits; i++) {
            hits[i] = SearchHitTests.createTestItem(false); // creating random innerHits could create loops
        }
        long totalHits = TestUtil.nextLong(random(), 0, Long.MAX_VALUE);
        TotalHits.Relation relation = randomFrom(TotalHits.Relation.values());
        float maxScore = frequently() ? randomFloat() : Float.NaN;

        return new SearchHits(hits, frequently() ? new TotalHits(totalHits, relation) : null, maxScore);
    }

    @Override
    protected SearchHits createBlankInstance() {
        return new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
    }

    @Override
    protected SearchHits createTestInstance() {
        return createTestItem();
    }

    public void testFromXContent() throws IOException {
        SearchHits searchHits = createTestItem();
        XContentType xcontentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(searchHits, xcontentType, ToXContent.EMPTY_PARAMS, humanReadable);
        SearchHits parsed;
        try (XContentParser parser = createParser(xcontentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(SearchHits.Fields.HITS, parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = SearchHits.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xcontentType, humanReadable), xcontentType);
    }

    /**
     * This test adds randomized fields on all json objects and checks that we
     * can parse it to ensure the parsing is lenient for forward compatibility.
     * We need to exclude json objects with the "highlight" and "fields" field
     * name since these objects allow arbitrary keys (the field names that are
     * queries). Also we want to exclude to add anything under "_source" since
     * it is not parsed.
     */
    public void testFromXContentLenientParsing() throws IOException {
        SearchHits searchHits = createTestItem();
        XContentType xcontentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toXContent(searchHits, xcontentType, ToXContent.EMPTY_PARAMS, true);
        Predicate<String> pathsToExclude = path -> (path.isEmpty() || path.endsWith("highlight") || path.endsWith("fields")
                || path.contains("_source"));
        BytesReference withRandomFields = insertRandomFields(xcontentType, originalBytes, pathsToExclude, random());
        SearchHits parsed = null;
        try (XContentParser parser = createParser(xcontentType.xContent(), withRandomFields)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(SearchHits.Fields.HITS, parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = SearchHits.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xcontentType, true), xcontentType);
    }
}
