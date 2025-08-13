/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.hasSize;

public class TimeBasedIndicesIT extends AbstractEsqlIntegTestCase {

    public void testFilter() {
        long epoch = System.currentTimeMillis();
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("@timestamp", "type=date", "value", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk("test").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        int oldDocs = between(10, 100);
        for (int i = 0; i < oldDocs; i++) {
            long timestamp = epoch - TimeValue.timeValueHours(between(1, 2)).millis();
            bulk.add(new IndexRequest().source("@timestamp", timestamp, "value", -i));
        }
        int newDocs = between(10, 100);
        for (int i = 0; i < newDocs; i++) {
            long timestamp = epoch + TimeValue.timeValueHours(between(1, 2)).millis();
            bulk.add(new IndexRequest().source("@timestamp", timestamp, "value", i));
        }
        bulk.get();
        {
            String query = "FROM test | limit 1000";
            var filter = new RangeQueryBuilder("@timestamp").from(epoch - TimeValue.timeValueHours(3).millis()).to("now");
            try (var resp = run(syncEsqlQueryRequest().query(query).filter(filter))) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(oldDocs));
            }
        }
        {
            String query = "FROM test | limit 1000";
            var filter = new RangeQueryBuilder("@timestamp").from("now").to(epoch + TimeValue.timeValueHours(3).millis());
            try (var resp = run(syncEsqlQueryRequest().query(query).filter(filter))) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(newDocs));
            }
        }
    }
}
