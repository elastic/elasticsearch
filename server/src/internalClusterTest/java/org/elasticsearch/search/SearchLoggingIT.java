/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.AutoCreateSystemIndexIT;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchLogProducer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.indices.TestSystemIndexDescriptor;
import org.elasticsearch.indices.TestSystemIndexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.AbstractSearchCancellationTestCase;
import org.elasticsearch.test.ActionLoggingUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.AbstractSearchCancellationTestCase.ScriptedBlockPlugin.SEARCH_BLOCK_SCRIPT_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class SearchLoggingIT extends AbstractSearchCancellationTestCase {
    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(SearchLogProducer.LOGGER_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void initAppender() throws IllegalAccessException {
        appender = new AccumulatingMockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(queryLog, appender);

        Loggers.setLevel(queryLog, Level.TRACE);
    }

    @AfterClass
    public static void cleanupAppender() {
        Loggers.removeAppender(queryLog, appender);
        appender.stop();

        Loggers.setLevel(queryLog, origQueryLogLevel);
    }

    @Before
    public void enableLog() {
        ActionLoggingUtils.enableLoggers();
        appender.reset();
    }

    @After
    public void restoreLog() {
        ActionLoggingUtils.disableLoggers();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class),
            AutoCreateSystemIndexIT.UnmanagedSystemIndexTestPlugin.class
        );
    }

    private static final String INDEX_NAME = "test_index";

    // Test _search
    public void testSearchLog() {
        setupIndex();

        // Simple request
        {
            assertSearchHitsWithoutFailures(prepareSearch().setQuery(simpleQueryStringQuery("fox")), "1");
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
            ESLogMessage message = (ESLogMessage) event.getMessage();
            var data = message.getIndexedReadOnlyStringMap();
            assertThat(message.get("success"), equalTo("true"));
            assertThat(message.get("type"), equalTo("search"));
            assertThat(message.get("hits"), equalTo("1"));
            assertThat(data.getValue("took"), greaterThan(0L));
            assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
            assertThat(message.get("query"), containsString("fox"));
            assertThat(message.get("indices"), equalTo(""));
        }

        // Match
        {
            assertSearchHitsWithoutFailures(prepareSearch(INDEX_NAME).setQuery(matchQuery("field1", "quick")), "1", "2", "3");
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
            ESLogMessage message = (ESLogMessage) event.getMessage();
            var data = message.getIndexedReadOnlyStringMap();
            assertThat(message.get("success"), equalTo("true"));
            assertThat(message.get("type"), equalTo("search"));
            assertThat(message.get("hits"), equalTo("3"));
            assertThat(data.getValue("took"), greaterThan(0L));
            assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
            assertThat(message.get("query"), containsString("quick"));
            assertThat(message.get("indices"), equalTo(INDEX_NAME));
        }
    }

    public void testFailureLog() {
        assertAcked(prepareCreate(INDEX_NAME).setMapping("field1", "type=text,index_options=docs"));
        indexRandom(
            true,
            prepareIndex(INDEX_NAME).setId("1").setSource("field1", "quick brown fox", "field2", "quick brown fox"),
            prepareIndex(INDEX_NAME).setId("2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox")
        );

        assertFailures(
            prepareSearch(INDEX_NAME).setQuery(matchPhraseQuery("field1", "quick brown").slop(0)),
            RestStatus.BAD_REQUEST,
            containsString("field:[field1] was indexed without position data; cannot run PhraseQuery")
        );
        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
        ESLogMessage message = (ESLogMessage) event.getMessage();
        var data = message.getIndexedReadOnlyStringMap();
        assertThat(message.get("success"), equalTo("false"));
        assertThat(message.get("type"), equalTo("search"));
        assertThat(message.get("hits"), equalTo("0"));
        assertThat(data.getValue("took"), greaterThan(0L));
        assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
        assertThat(message.get("query"), containsString("quick brown"));
        assertThat(message.get("indices"), equalTo(INDEX_NAME));
        assertThat(message.get("error.type"), equalTo("org.elasticsearch.action.search.SearchPhaseExecutionException"));
        assertThat(message.get("error.message"), equalTo("all shards failed"));
    }

    public void testSearchCancel() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        ActionFuture<SearchResponse> searchResponse = prepareSearch("test").addScriptField(
            "test_field",
            new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())
        ).setAllowPartialSearchResults(false).execute();

        awaitForBlock(plugins);
        cancelSearch(TransportSearchAction.TYPE.name());
        disableBlocks(plugins);
        ensureSearchWasCancelled(searchResponse);
        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
        ESLogMessage message = (ESLogMessage) event.getMessage();
        var data = message.getIndexedReadOnlyStringMap();
        assertThat(message.get("success"), equalTo("false"));
        assertThat(message.get("type"), equalTo("search"));
        assertThat(message.get("hits"), equalTo("0"));
        assertThat(data.getValue("took"), greaterThan(0L));
        assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
        assertThat(message.get("query"), containsString("mockscript"));
        assertThat(message.get("indices"), equalTo("test"));
        assertThat(message.get("error.type"), equalTo("org.elasticsearch.action.search.SearchPhaseExecutionException"));
    }

    public void testMultiSearch() {
        setupIndex();

        var request = client().prepareMultiSearch()
            .add(prepareSearch(INDEX_NAME).setQuery(matchQuery("field1", "quick")))
            .add(prepareSearch(INDEX_NAME).setQuery(matchQuery("field1", "fox")));
        assertResponse(request, res -> assertThat(res.getResponses().length, equalTo(2)));
        assertThat(appender.events, hasSize(2));

        appender.events.forEach(ev -> {
            assertThat(ev.getMessage(), instanceOf(ESLogMessage.class));
            ESLogMessage message = (ESLogMessage) ev.getMessage();
            var data = message.getIndexedReadOnlyStringMap();
            assertThat(message.get("success"), equalTo("true"));
            assertThat(message.get("type"), equalTo("search"));
            assertThat(data.getValue("took"), greaterThan(0L));
            assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
            assertThat(message.get("indices"), equalTo(INDEX_NAME));
            if (message.get("query").contains("quick")) {
                assertThat(message.get("hits"), equalTo("3"));
            } else if (message.get("query").contains("fox")) {
                assertThat(message.get("hits"), equalTo("1"));
            } else {
                fail("unexpected query logged: " + message.get("query"));
            }
        });
    }

    public void testPitSearch() {
        setupIndex();

        OpenPointInTimeRequest request = new OpenPointInTimeRequest(INDEX_NAME).keepAlive(TimeValue.THIRTY_SECONDS);
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        var pitId = response.getPointInTimeId();
        try {
            assertSearchHitsWithoutFailures(
                prepareSearch().setQuery(simpleQueryStringQuery("fox")).setPointInTime(new PointInTimeBuilder(pitId)),
                "1"
            );
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
            ESLogMessage message = (ESLogMessage) event.getMessage();
            var data = message.getIndexedReadOnlyStringMap();
            assertThat(message.get("success"), equalTo("true"));
            assertThat(message.get("type"), equalTo("search"));
            assertThat(message.get("hits"), equalTo("1"));
            assertThat(data.getValue("took"), greaterThan(0L));
            assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
            assertThat(message.get("query"), containsString("fox"));
            assertThat(message.get("indices"), equalTo(INDEX_NAME));
        } finally {
            response.decRef();
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
    }

    public void testLogFiltering() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(TestSystemIndexDescriptor.PRIMARY_INDEX_NAME);
        client().execute(AutoCreateAction.INSTANCE, request).get();
        // Log empty search
        assertResponse(
            prepareSearch(SearchLogProducer.NEVER_MATCH).setQuery(new MatchAllQueryBuilder()),
            ElasticsearchAssertions::assertNoFailures
        );
        assertNull(appender.getLastEventAndReset());
        // Log system index
        assertResponse(
            prepareSearch(TestSystemIndexDescriptor.PRIMARY_INDEX_NAME).setQuery(new MatchAllQueryBuilder()),
            ElasticsearchAssertions::assertNoFailures
        );
        assertNull(appender.getLastEventAndReset());
        // Log system index with option on
        ActionLoggingUtils.enableLoggingSystem();
        try {
            assertResponse(
                prepareSearch(TestSystemIndexDescriptor.PRIMARY_INDEX_NAME).setQuery(new MatchAllQueryBuilder()),
                ElasticsearchAssertions::assertNoFailures
            );
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
            ESLogMessage message = (ESLogMessage) event.getMessage();
            assertThat(message.get("indices"), equalTo(TestSystemIndexDescriptor.PRIMARY_INDEX_NAME));
        } finally {
            ActionLoggingUtils.disableLoggingSystem();
        }
    }

    private void setupIndex() {
        createIndex(INDEX_NAME);
        indexRandom(
            true,
            prepareIndex(INDEX_NAME).setId("1").setSource("field1", "the quick brown fox jumps"),
            prepareIndex(INDEX_NAME).setId("2").setSource("field1", "quick brown"),
            prepareIndex(INDEX_NAME).setId("3").setSource("field1", "quick")
        );
    }

}
