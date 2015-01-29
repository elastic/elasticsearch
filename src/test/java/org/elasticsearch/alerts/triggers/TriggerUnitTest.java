/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.triggers;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class TriggerUnitTest extends ElasticsearchTestCase {

    private XContentBuilder createTriggerContent(String script, String scriptLang, ScriptService.ScriptType scriptType) throws IOException {
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field("script");
        jsonBuilder.startObject();
        jsonBuilder.field("script", script);
        if (scriptLang != null) {
            jsonBuilder.field("script_lang", scriptLang);
        }
        if (scriptType != null) {
            jsonBuilder.field("script_type", scriptType.toString());
        }
        jsonBuilder.endObject();
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testInlineScriptTriggers() throws Exception {

        Settings settings = ImmutableSettings.settingsBuilder().build();
        GroovyScriptEngineService groovyScriptEngineService = new GroovyScriptEngineService(settings);
        ThreadPool tp = new ThreadPool(ThreadPool.Names.SAME);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(groovyScriptEngineService);

        ScriptService scriptService = new ScriptService(settings, new Environment(), engineServiceSet, new ResourceWatcherService(settings, tp));
        TriggerService triggerService = new TriggerService(settings, null, scriptService);

        try {
            XContentBuilder builder = createTriggerContent("hits.total > 1", null, null);
            XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
            AlertTrigger trigger = triggerService.instantiateAlertTrigger(parser);

            SearchRequest request = new SearchRequest();
            request.indices("my-index");
            request.types("my-type");


            SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500l, new ShardSearchFailure[0]);
            XContentBuilder responseBuilder = jsonBuilder().startObject().value(response).endObject();
            Map<String, Object> responseMap = XContentHelper.convertToMap(responseBuilder.bytes(), false).v2();
            assertFalse(triggerService.isTriggered(trigger, request, responseMap).isTriggered());


            builder = createTriggerContent("return true", null, null);
            parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
            trigger = triggerService.instantiateAlertTrigger(parser);

            assertTrue(triggerService.isTriggered(trigger, request, responseMap).isTriggered());


            tp.shutdownNow();

        } catch (IOException ioe) {
            throw new ElasticsearchException("Failed to construct the trigger", ioe);
        }
    }

}
