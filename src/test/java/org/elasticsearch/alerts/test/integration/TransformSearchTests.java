/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.scheduler.schedule.IntervalSchedule.Interval;
import org.elasticsearch.alerts.test.AbstractAlertsIntegrationTests;
import org.elasticsearch.alerts.test.AlertsTestUtils;
import org.elasticsearch.alerts.transform.SearchTransform;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.alerts.actions.ActionBuilders.indexAction;
import static org.elasticsearch.alerts.client.AlertSourceBuilder.alertSourceBuilder;
import static org.elasticsearch.alerts.input.InputBuilders.searchInput;
import static org.elasticsearch.alerts.scheduler.schedule.Schedules.interval;
import static org.elasticsearch.alerts.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.*;

/**
 */
public class TransformSearchTests extends AbstractAlertsIntegrationTests {

    @Test
    public void testTransformSearchRequest() throws Exception {
        createIndex("my-condition-index", "my-payload-index", "my-payload-output");
        ensureGreen("my-condition-index", "my-payload-index", "my-payload-output");

        index("my-payload-index", "payload", "mytestresult");
        refresh();

        SearchRequest inputRequest = AlertsTestUtils.newInputSearchRequest("my-condition-index").source(searchSource().query(matchAllQuery()));
        SearchRequest transformRequest = AlertsTestUtils.newInputSearchRequest("my-payload-index").source(searchSource().query(matchAllQuery()));
        transformRequest.searchType(SearchTransform.DEFAULT_SEARCH_TYPE);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("list", "baz");

        PutAlertResponse putAlertResponse = alertClient().preparePutAlert("test-payload")
                .source(alertSourceBuilder()
                        .schedule(interval(5, Interval.Unit.SECONDS))
                        .input(searchInput(inputRequest))
                        .transform(searchTransform(transformRequest))
                        .addAction(indexAction("my-payload-output", "result"))
                        .metadata(metadata)
                        .throttlePeriod(TimeValue.timeValueSeconds(0)))
                .get();
        assertThat(putAlertResponse.indexResponse().isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().scheduler().fire("test-payload");
            refresh();
        }

        assertAlertWithMinimumPerformedActionsCount("test-payload", 1, false);
        refresh();

        SearchRequest searchRequest = client().prepareSearch("my-payload-output").request();
        searchRequest.source(searchSource().query(matchAllQuery()));
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));
        SearchHit hit = searchResponse.getHits().getHits()[0];
        String source = hit.getSourceRef().toUtf8();

        assertThat(source, containsString("mytestresult"));
    }
}
