/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.alerts.triggers.ScriptTrigger;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
*/
public class EmailTemplateTest extends ElasticsearchTestCase {

    public void testEmailTemplateRender() throws IOException {
        //createIndex("my-trigger-index");
        SearchRequest triggerRequest = new SearchRequest();

        List<AlertAction> actions = new ArrayList<>();


        Alert alert = new Alert("test-email-template",
                triggerRequest,
                new ScriptTrigger("return true", ScriptService.ScriptType.INLINE, "groovy"),
                actions,
                "0/5 * * * * ? *",
                new DateTime(),
                0,
                new TimeValue(0),
                Alert.Status.NOT_TRIGGERED);

        SearchResponse searchResponse = new SearchResponse(InternalSearchResponse.empty(),"",0,0,0L, new ShardSearchFailure[0]);

        XContentBuilder responseBuilder = jsonBuilder().startObject().value(searchResponse).endObject();
        Map<String, Object> responseMap = XContentHelper.convertToMap(responseBuilder.bytes(), false).v2();

        TriggerResult result = new TriggerResult(true, triggerRequest, responseMap, alert.getTrigger());

        String template = "{{alert_name}} triggered with {{response.hits.total}} hits";

        Settings settings = ImmutableSettings.settingsBuilder().build();
        MustacheScriptEngineService mustacheScriptEngineService = new MustacheScriptEngineService(settings);
        ThreadPool tp;
        tp = new ThreadPool(ThreadPool.Names.SAME);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(mustacheScriptEngineService);

        ScriptService scriptService = new ScriptService(settings, new Environment(), engineServiceSet, new ResourceWatcherService(settings, tp));
        String parsedTemplate = SmtpAlertActionFactory.renderTemplate(template, alert, result, ScriptServiceProxy.of(scriptService));
        tp.shutdownNow();
        assertEquals("test-email-template triggered with 0 hits", parsedTemplate);

    }

}
