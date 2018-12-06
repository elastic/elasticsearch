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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Predicate;

public class SearchHitsTests extends AbstractStreamableXContentTestCase<SearchHits> {
    public static SearchHits createTestItem(boolean withOptionalInnerHits, boolean withShardTarget) {
        return createTestItem(randomFrom(XContentType.values()), withOptionalInnerHits, withShardTarget);
    }

    private static SearchHit[] createSearchHitArray(int size, XContentType xContentType, boolean withOptionalInnerHits,
                                                    boolean withShardTarget) {
        SearchHit[] hits = new SearchHit[size];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = SearchHitTests.createTestItem(xContentType, withOptionalInnerHits, withShardTarget);
        }
        return hits;
    }

    private static TotalHits randomTotalHits() {
        long totalHits = TestUtil.nextLong(random(), 0, Long.MAX_VALUE);
        TotalHits.Relation relation = randomFrom(TotalHits.Relation.values());
        return new TotalHits(totalHits, relation);
    }

    public static SearchHits createTestItem(XContentType xContentType, boolean withOptionalInnerHits, boolean withShardTarget) {
        int searchHits = randomIntBetween(0, 5);
        SearchHit[] hits = createSearchHitArray(searchHits, xContentType, withOptionalInnerHits, withShardTarget);
        float maxScore = frequently() ? randomFloat() : Float.NaN;
        return new SearchHits(hits, frequently() ? randomTotalHits() : null, maxScore);
    }

    @Override
    protected SearchHits mutateInstance(SearchHits instance) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return new SearchHits(createSearchHitArray(instance.getHits().length + 1,
                    randomFrom(XContentType.values()), false, randomBoolean()),
                    instance.getTotalHits(), instance.getMaxScore());
            case 1:
                final TotalHits totalHits;
                if (instance.getTotalHits() == null) {
                    totalHits = randomTotalHits();
                } else {
                    totalHits = null;
                }
                return new SearchHits(instance.getHits(), totalHits, instance.getMaxScore());
            case 2:
                final float maxScore;
                if (Float.isNaN(instance.getMaxScore())) {
                    maxScore = randomFloat();
                } else {
                    maxScore = Float.NaN;
                }
                return new SearchHits(instance.getHits(), instance.getTotalHits(), maxScore);
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return path -> (path.isEmpty() ||
            path.contains("inner_hits") || path.contains("highlight") || path.contains("fields") || path.contains("_source"));
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] {"_source"};
    }

    @Override
    protected SearchHits createBlankInstance() {
        return new SearchHits();
    }

    @Override
    protected SearchHits createTestInstance() {
        return createTestItem(randomFrom(XContentType.values()), true, true);
    }

    @Override
    protected SearchHits createXContextTestInstance(XContentType xContentType) {
        return createTestItem(xContentType,true, false);
    }

    @Override
    protected SearchHits doParseInstance(XContentParser parser) throws IOException {
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(SearchHits.Fields.HITS, parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        SearchHits searchHits = SearchHits.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return searchHits;
    }

    public void testToXContent() throws IOException {
        SearchHit[] hits = new SearchHit[] {
            new SearchHit(1, "id1", new Text("type"), Collections.emptyMap()),
            new SearchHit(2, "id2", new Text("type"), Collections.emptyMap()) };

        long totalHits = 1000;
        float maxScore = 1.5f;
        SearchHits searchHits = new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), maxScore);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        searchHits.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"hits\":{\"total\":{\"value\":1000,\"relation\":\"eq\"},\"max_score\":1.5," +
            "\"hits\":[{\"_type\":\"type\",\"_id\":\"id1\",\"_score\":null},"+
            "{\"_type\":\"type\",\"_id\":\"id2\",\"_score\":null}]}}", Strings.toString(builder));
    }
}
