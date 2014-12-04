/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.IndexAlertAction;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.alerts.triggers.ScriptedTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
public class PayloadSearchTest extends AbstractAlertingTests {

    @Test
    public void testPayloadSearchRequest() throws Exception {
        createIndex("my-trigger-index", "my-payload-index", "my-payload-output");
        ensureGreen("my-trigger-index", "my-payload-index", "my-payload-output");

        index("my-payload-index","payload", "mytestresult");
        refresh();

        SearchRequest triggerRequest = createTriggerSearchRequest("my-trigger-index").source(searchSource().query(matchAllQuery()));
        SearchRequest payloadRequest = createTriggerSearchRequest("my-payload-index").source(searchSource().query(matchAllQuery()));
        payloadRequest.searchType(AlertUtils.DEFAULT_PAYLOAD_SEARCH_TYPE);

        List<AlertAction> actions = new ArrayList<>();
        actions.add(new IndexAlertAction("my-payload-output","result"));
        Alert alert = new Alert("test-payload",
                triggerRequest,
                new ScriptedTrigger("return true", ScriptService.ScriptType.INLINE, "groovy"),
                actions,
                "0/5 * * * * ? *",
                new DateTime(),
                0,
                new TimeValue(0),
                AlertAckState.NOT_ACKABLE);

        alert.setPayloadSearchRequest(payloadRequest);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        PutAlertResponse putAlertResponse = alertClient().preparePutAlert("test-payload").setAlertSource(jsonBuilder.bytes()).get();
        assertTrue(putAlertResponse.indexResponse().isCreated());

        assertAlertTriggered("test-payload", 1, false);
        refresh();
        SearchRequest searchRequest = client().prepareSearch("my-payload-output").request();
        searchRequest.source(searchSource().query(matchAllQuery()));
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));
        SearchHit hit = searchResponse.getHits().getHits()[0];
        String source = hit.getSourceRef().toUtf8();

        assertTrue(source.contains("mytestresult"));
    }
}
