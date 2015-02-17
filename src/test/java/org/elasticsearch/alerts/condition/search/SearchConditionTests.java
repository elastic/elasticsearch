/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.alerts.AbstractAlertingTests;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class SearchConditionTests extends ElasticsearchTestCase {

    private XContentBuilder createConditionContent(String script, String scriptLang, ScriptService.ScriptType scriptType, SearchRequest request) throws IOException {
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
        jsonBuilder.field(ScriptSearchCondition.Parser.REQUEST_FIELD.getPreferredName());
        AlertUtils.writeSearchRequest(request, jsonBuilder, ToXContent.EMPTY_PARAMS);
        jsonBuilder.endObject();
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testInlineScriptConditions() throws Exception {

        Settings settings = ImmutableSettings.settingsBuilder().build();
        GroovyScriptEngineService groovyScriptEngineService = new GroovyScriptEngineService(settings);
        ThreadPool tp = new ThreadPool(ThreadPool.Names.SAME);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(groovyScriptEngineService);

        ScriptService scriptService = new ScriptService(settings, new Environment(), engineServiceSet, new ResourceWatcherService(settings, tp));
        ScriptSearchCondition.Parser conditionParser = new ScriptSearchCondition.Parser(settings, null, ScriptServiceProxy.of(scriptService));

        try {
            XContentBuilder builder = createConditionContent("hits.total > 1", null, null, AbstractAlertingTests.createConditionSearchRequest());
            XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
            ScriptSearchCondition condition = conditionParser.parse(parser);

            SearchRequest request = new SearchRequest();
            request.indices("my-index");
            request.types("my-type");


            SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), "", 3, 3, 500l, new ShardSearchFailure[0]);
            XContentBuilder responseBuilder = jsonBuilder().startObject().value(response).endObject();
            assertFalse(condition.processSearchResponse(response).met());


            builder = createConditionContent("return true", null, null, AbstractAlertingTests.createConditionSearchRequest());
            parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
            condition = conditionParser.parse(parser);

            assertTrue(condition.processSearchResponse(response).met());

            tp.shutdownNow();
        } catch (IOException ioe) {
            throw new ElasticsearchException("Failed to construct the condition", ioe);
        }
    }

}
