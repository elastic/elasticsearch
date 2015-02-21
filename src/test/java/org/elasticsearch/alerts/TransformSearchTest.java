/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.actions.index.IndexAction;
import org.elasticsearch.alerts.condition.search.ScriptSearchCondition;
import org.elasticsearch.alerts.scheduler.schedule.CronSchedule;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.transform.SearchTransform;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
public class TransformSearchTest extends AbstractAlertingTests {

    @Test
    public void testTransformSearchRequest() throws Exception {
        createIndex("my-condition-index", "my-payload-index", "my-payload-output");
        ensureGreen("my-condition-index", "my-payload-index", "my-payload-output");

        index("my-payload-index","payload", "mytestresult");
        refresh();

        SearchRequest conditionRequest = createConditionSearchRequest("my-condition-index").source(searchSource().query(matchAllQuery()));
        SearchRequest transformRequest = createConditionSearchRequest("my-payload-index").source(searchSource().query(matchAllQuery()));
        transformRequest.searchType(SearchTransform.DEFAULT_SEARCH_TYPE);

        List<Action> actions = new ArrayList<>();
        actions.add(new IndexAction(logger, ClientProxy.of(client()), "my-payload-output","result"));

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("list", "baz");

        Alert alert = new Alert(
                "test-serialization",
                new CronSchedule("0/5 * * * * ? *"),
                new ScriptSearchCondition(logger, scriptService(), ClientProxy.of(client()),
                        conditionRequest,"return true", ScriptService.ScriptType.INLINE, "groovy"),
                new SearchTransform(logger, scriptService(), ClientProxy.of(client()), transformRequest),
                new TimeValue(0),
                new Actions(actions),
                metadata,
                new Alert.Status()
        );

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        PutAlertResponse putAlertResponse = alertClient().preparePutAlert("test-payload").setAlertSource(jsonBuilder.bytes()).get();
        assertTrue(putAlertResponse.indexResponse().isCreated());

        assertAlertWithMinimumPerformedActionsCount("test-payload", 1, false);
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
