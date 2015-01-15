/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertAckState;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.alerts.triggers.ScriptedTrigger;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
 */
public class WebhookTest extends ElasticsearchTestCase {

    public void testRequestParameterSerialization() throws Exception {
        SearchRequest triggerRequest = (new SearchRequest()).source(searchSource().query(matchAllQuery()));
        AlertTrigger trigger = new ScriptedTrigger("return true", ScriptService.ScriptType.INLINE, "groovy");
        List<AlertAction> actions = new ArrayList<>();

        Alert alert = new Alert("test-email-template",
                triggerRequest,
                trigger,
                actions,
                "0/5 * * * * ? *",
                new DateTime(),
                0,
                new TimeValue(0),
                AlertAckState.NOT_TRIGGERED);



        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("hits",0);

        Settings settings = ImmutableSettings.settingsBuilder().build();
        MustacheScriptEngineService mustacheScriptEngineService = new MustacheScriptEngineService(settings);
        ThreadPool tp;
        tp = new ThreadPool(ThreadPool.Names.SAME);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(mustacheScriptEngineService);

        ScriptService scriptService = new ScriptService(settings, new Environment(), engineServiceSet, new ResourceWatcherService(settings, tp));


        String encodedRequestParameters = WebhookAlertActionFactory.encodeParameterString(WebhookAlertActionFactory.DEFAULT_PARAMETER_STRING, alert,
                new TriggerResult(true, triggerRequest, responseMap, trigger), scriptService);
        assertEquals("alertname=test-email-template&request=%%7B%22query%22%3A%7B%22match_all%22%3A%7B%7D%7D%7D&response=%%7B%22response%22%3A%7B%22hits%22%3A0%7D%7D", encodedRequestParameters);
        tp.shutdownNow();

    }
}
