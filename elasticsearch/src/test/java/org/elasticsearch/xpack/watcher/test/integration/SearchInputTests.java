/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.input.Input;
import org.elasticsearch.xpack.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInputFactory;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.joda.time.DateTimeZone.UTC;

@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, supportsDedicatedMasters = false,
        numDataNodes = 1)
public class SearchInputTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> types = new ArrayList<>();
        types.addAll(super.nodePlugins());
        types.add(MockMustacheScriptEngine.TestPlugin.class);
        types.add(CustomScriptContextPlugin.class);
        return types;
    }

    public void testExecute() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                boolQuery().must(matchQuery("event_type", "a")));

        WatcherSearchTemplateRequest request = WatcherTestUtils.templateRequest(searchSourceBuilder);
        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null, null, null), logger,
                WatcherClientProxy.of(client()), watcherSearchTemplateService(), null);
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        AlwaysCondition.INSTANCE,
                        null,
                        null,
                        new ArrayList<>(),
                        null,
                        new WatchStatus(new DateTime(0, UTC), emptyMap())),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(0, UTC), new DateTime(0, UTC)),
                timeValueSeconds(5));
        SearchInput.Result result = searchInput.execute(ctx, new Payload.Simple());

        assertThat(XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertThat(result.status(), is(Input.Result.Status.SUCCESS));
        assertEquals(result.executedRequest().getSearchType(), request.getSearchType());
        assertArrayEquals(result.executedRequest().getIndices(), request.getIndices());
        assertEquals(result.executedRequest().getIndicesOptions(), request.getIndicesOptions());
    }

    public void testDifferentSearchType() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                boolQuery().must(matchQuery("event_type", "a"))
        );
        SearchType searchType = getRandomSupportedSearchType();
        WatcherSearchTemplateRequest request = WatcherTestUtils.templateRequest(searchSourceBuilder, searchType);

        ExecutableSearchInput searchInput = new ExecutableSearchInput(new SearchInput(request, null, null, null), logger,
                WatcherClientProxy.of(client()), watcherSearchTemplateService(), null);
        WatchExecutionContext ctx = new TriggeredExecutionContext(
                new Watch("test-watch",
                        new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                        new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                        AlwaysCondition.INSTANCE,
                        null,
                        null,
                        new ArrayList<>(),
                        null,
                        new WatchStatus(new DateTime(0, UTC), emptyMap())),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent("test-watch", new DateTime(0, UTC), new DateTime(0, UTC)),
                timeValueSeconds(5));
        SearchInput.Result result = searchInput.execute(ctx, new Payload.Simple());

        assertThat(XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.executedRequest());
        assertThat(result.status(), is(Input.Result.Status.SUCCESS));
        assertEquals(result.executedRequest().getSearchType(), searchType);
        assertArrayEquals(result.executedRequest().getIndices(), request.getIndices());
        assertEquals(result.executedRequest().getIndicesOptions(), request.getIndicesOptions());
    }

    public void testParserValid() throws Exception {
        SearchSourceBuilder source = searchSource()
                        .query(boolQuery().must(matchQuery("event_type", "a")).must(rangeQuery("_timestamp")
                                .from("{{ctx.trigger.scheduled_time}}||-30s").to("{{ctx.trigger.triggered_time}}")));

        TimeValue timeout = randomBoolean() ? TimeValue.timeValueSeconds(randomInt(10)) : null;
        XContentBuilder builder = jsonBuilder().value(
                new SearchInput(WatcherTestUtils.templateRequest(source), null, timeout, null));
        XContentParser parser = createParser(builder);
        parser.nextToken();

        SearchInputFactory factory = new SearchInputFactory(Settings.EMPTY, WatcherClientProxy.of(client()),
                                                            xContentRegistry(), scriptService());

        SearchInput searchInput = factory.parseInput("_id", parser);
        assertEquals(SearchInput.TYPE, searchInput.type());
        assertThat(searchInput.getTimeout(), equalTo(timeout));
    }

    private WatcherSearchTemplateService watcherSearchTemplateService() {
        String master = internalCluster().getMasterName();
        return new WatcherSearchTemplateService(internalCluster().clusterService(master).getSettings(),
                internalCluster().getInstance(ScriptService.class, master),
                internalCluster().getInstance(NamedXContentRegistry.class, master)
        );
    }

    private ScriptService scriptService() {
        return internalCluster().getInstance(ScriptService.class);
    }

    /**
     * Custom plugin that registers XPack script context.
     */
    public static class CustomScriptContextPlugin extends Plugin implements ScriptPlugin {

        @Override
        public ScriptContext.Plugin getCustomScriptContexts() {
            return new ScriptContext.Plugin("xpack", "watch");
        }
    }
}
