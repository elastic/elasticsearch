/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.sort.SortBuilders;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class SqlTranslateActionIT extends AbstractSqlIntegTestCase {

    public void testSqlTranslateAction() {
        assertAcked(indicesAdmin().prepareCreate("test").get());
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("data", "bar", "count", 42, "date", "1984-01-04"))
            .add(new IndexRequest("test").id("2").source("data", "baz", "count", 43, "date", "1989-12-19"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("test");

        boolean columnOrder = randomBoolean();
        String columns = columnOrder ? "data, count, date" : "date, data, count";
        SqlTranslateResponse response = new SqlTranslateRequestBuilder(client(), SqlTranslateAction.INSTANCE).query(
            "SELECT " + columns + " FROM test ORDER BY count"
        ).get();
        SearchSourceBuilder source = response.source();
        List<FieldAndFormat> actualFields = source.fetchFields();
        List<FieldAndFormat> expectedFields = new ArrayList<>(3);
        if (columnOrder) {
            expectedFields.add(new FieldAndFormat("data", null));
            expectedFields.add(new FieldAndFormat("count", null));
            expectedFields.add(new FieldAndFormat("date", "strict_date_optional_time_nanos"));
        } else {
            expectedFields.add(new FieldAndFormat("date", "strict_date_optional_time_nanos"));
            expectedFields.add(new FieldAndFormat("data", null));
            expectedFields.add(new FieldAndFormat("count", null));
        }
        assertEquals(expectedFields, actualFields);
        assertEquals(singletonList(SortBuilders.fieldSort("count").missing("_last").unmappedType("long")), source.sorts());
    }
}
