/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.search.SortField;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

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
        String scoreSortString = Strings.format("""
            { "_score": { "order": "%s" }}
            """, order.toString());
        XContentParser parser = createParser(JsonXContent.jsonXContent, scoreSortString);
        // need to skip until parser is located on second START_OBJECT
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        ScoreSortBuilder scoreSort = ScoreSortBuilder.fromXContent(parser, "_score");
        assertEquals(order, scoreSort.order());
    }

    public void testReverseOptionFails() throws IOException {
        String json = """
            { "_score": { "reverse": true }}""";
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
