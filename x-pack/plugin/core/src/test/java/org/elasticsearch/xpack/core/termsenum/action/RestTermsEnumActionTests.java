/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.CCSVersionCheckHelper;
import org.elasticsearch.search.FailBeforeVersionQueryBuilder;
import org.elasticsearch.search.NewlyReleasedQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry.Entry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.termsenum.rest.RestTermsEnumAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestTermsEnumActionTests extends ESTestCase {

    private static ThreadPool threadPool = new TestThreadPool(RestTermsEnumActionTests.class.getName());
    private static NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

    private static UsageService usageService = new UsageService();
    private static RestController controller = new RestController(emptySet(), null, client, new NoneCircuitBreakerService(), usageService);
    private static RestTermsEnumAction action = new RestTermsEnumAction();

    /**
     * Configures {@link NodeClient} to stub {@link TermsEnumAction} transport action.
     * <p>
     * This lower level of execution is out of the scope of this test.
     */
    @BeforeClass
    public static void stubTermEnumAction() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());

        final TransportAction<? extends ActionRequest, ? extends ActionResponse> transportAction = new TransportAction<>(
            TermsEnumAction.NAME,
            new ActionFilters(Collections.emptySet()),
            taskManager
        ) {
            @Override
            protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {}
        };

        final Map<ActionType<? extends ActionResponse>, TransportAction<? extends ActionRequest, ? extends ActionResponse>> actions =
            new HashMap<>();
        actions.put(TermsEnumAction.INSTANCE, transportAction);

        client.initialize(
            actions,
            taskManager,
            () -> "local",
            mock(Transport.Connection.class),
            null,
            new NamedWriteableRegistry(List.of())
        );
        controller.registerHandler(action);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                new ParseField(FailBeforeVersionQueryBuilder.NAME),
                FailBeforeVersionQueryBuilder::fromXContent,
                RestApiVersion.onOrAfter(RestApiVersion.current())
            )
        );
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                new ParseField(NewlyReleasedQueryBuilder.NAME),
                NewlyReleasedQueryBuilder::fromXContent,
                RestApiVersion.onOrAfter(RestApiVersion.current())
            )
        );
        return new NamedXContentRegistry(namedXContents);
    }

    @AfterClass
    public static void terminateThreadPool() {
        terminate(threadPool);

        threadPool = null;
        client = null;

        usageService = null;
        controller = null;
        action = null;
    }

    public void testRestTermEnumAction() throws Exception {
        // GIVEN a valid query
        final String content = """
            {
              "field": "a",
              "string": "foo",
              "search_after": "football",
              "index_filter": {
                "bool": {
                  "must": {
                    "term": {
                      "user": "kimchy"
                    }
                  }
                }
              }
            }""";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN request is parsed OK
        assertThat(channel.responses().get(), equalTo(0));
        assertThat(channel.errors().get(), equalTo(0));
        assertNull(channel.capturedResponse());
    }

    public void testRestTermEnumActionMissingField() throws Exception {
        // GIVEN an invalid query
        final String content = """
            {
              "string": "foo",
              "index_filter": {
                "bool": {
                  "must": {
                    "term": {
                      "user": "kimchy"
                    }
                  }
                }
              }
            }""";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN request is invalid - missing mandatory "field" parameter.
        assertThat(channel.responses().get(), equalTo(0));
        assertThat(channel.errors().get(), equalTo(1));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("field cannot be null"));
    }

    private RestRequest createRestRequest(String content) {
        return new FakeRestRequest.Builder(xContentRegistry()).withPath("index1/_terms_enum")
            .withParams(emptyMap())
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }

    public void testCCSCheckCompatibilityFlag() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG, "true");

        String query = """
            { "field" : "test", "index_filter" : { "fail_before_current_version" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index/_terms_enum")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, client));
            assertEquals(
                "parts of request [POST /some_index/_terms_enum] are not compatible with version 8.0.0 and the 'check_ccs_compatibility' "
                    + "is enabled.",
                ex.getMessage()
            );
            assertEquals("This query isn't serializable to nodes on or before 8.0.0", ex.getCause().getMessage());
        }

        String newQueryBuilderInside = """
            { "field" : "test", "index_filter" : { "new_released_query" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index/_terms_enum")
                .withParams(params)
                .withContent(new BytesArray(newQueryBuilderInside), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, client));
            assertEquals(
                "parts of request [POST /some_index/_terms_enum] are not compatible with version 8.0.0 and the 'check_ccs_compatibility' "
                    + "is enabled.",
                ex.getMessage()
            );
            assertEquals(
                "NamedWritable [org.elasticsearch.search.NewlyReleasedQueryBuilder] was released in "
                    + "version 8.1.0 and was not supported in version 8.0.0",
                ex.getCause().getMessage()
            );
        }

        // this shouldn't fail without the flag enabled
        params = new HashMap<>();
        if (randomBoolean()) {
            params.put(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG, "false");
        }
        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index//_terms_enum")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();
            action.prepareRequest(request, client);
        }
    }

}
