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

package org.elasticsearch.search.sort;


import org.apache.lucene.search.SortField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

public class ScoreSortBuilderTests extends AbstractSortTestCase<ScoreSortBuilder> {

    @Override
    protected ScoreSortBuilder createTestItem() {
        return randomScoreSortBuilder();
    }

    public static ScoreSortBuilder randomScoreSortBuilder() {
        return new ScoreSortBuilder().order(randomBoolean() ? SortOrder.ASC : SortOrder.DESC);
    }

    @Override
    protected ScoreSortBuilder mutate(ScoreSortBuilder original) throws IOException {
        ScoreSortBuilder result = new ScoreSortBuilder();
        result.order(randomValueOtherThan(original.order(), () -> randomFrom(SortOrder.values())));
        return result;
    }

    /**
     * test passing null to {@link ScoreSortBuilder#order(SortOrder)} is illegal
     */
    public void testIllegalOrder() {
        Exception e = expectThrows(NullPointerException.class, () -> new ScoreSortBuilder().order(null));
        assertEquals("sort order cannot be null.", e.getMessage());
    }

    /**
     * test parsing order parameter if specified as `order` field in the json
     * instead of the `reverse` field that we render in toXContent
     */
    public void testParseOrder() throws IOException {
        SortOrder order = randomBoolean() ? SortOrder.ASC : SortOrder.DESC;
        String scoreSortString = "{ \"_score\": { \"order\": \""+ order.toString() +"\" }}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scoreSortString);
        // need to skip until parser is located on second START_OBJECT
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        ScoreSortBuilder scoreSort = ScoreSortBuilder.fromXContent(parser, "_score");
        assertEquals(order, scoreSort.order());
    }

    public void testReverseOptionFails() throws IOException {
        String json = "{ \"_score\": { \"reverse\": true }}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        // need to skip until parser is located on second START_OBJECT
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        try {
          ScoreSortBuilder.fromXContent(parser, "_score");
          fail("adding reverse sorting option should fail with an exception");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    @Override
    protected void sortFieldAssertions(ScoreSortBuilder builder, SortField sortField, DocValueFormat format) {
        assertEquals(SortField.Type.SCORE, sortField.getType());
        assertEquals(builder.order() == SortOrder.DESC ? false : true, sortField.getReverse());
    }

    @Override
    protected ScoreSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return ScoreSortBuilder.fromXContent(parser, fieldName);
    }
}
