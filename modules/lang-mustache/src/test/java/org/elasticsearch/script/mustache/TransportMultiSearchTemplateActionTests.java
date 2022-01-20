/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.FailBeforeVersionQueryBuilder;
import org.elasticsearch.search.NewlyReleasedQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.junit.BeforeClass;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiSearchTemplateActionTests extends ESTestCase {

    private static NamedXContentRegistry xContentRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<org.elasticsearch.xcontent.NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
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
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    public void testCCSCompatibilityCheck() throws Exception {
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchTemplateActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            TransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);

            SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
            searchTemplateRequest.setScriptType(ScriptType.INLINE);
            searchTemplateRequest.setCcsCompatibilityCheck(true);
            searchTemplateRequest.setRequest(new SearchRequest());
            String query = """
                { "query": { "fail_before_current_version" : { }}}
                """;
            searchTemplateRequest.setScript(query);

            MultiSearchTemplateRequest mstr = new MultiSearchTemplateRequest();
            mstr.add(searchTemplateRequest);
            NodeClient client = new NodeClient(settings, threadPool);

            TransportMultiSearchTemplateAction action = new TransportMultiSearchTemplateAction(
                transportService,
                actionFilters,
                TestTemplateService.instance(),
                xContentRegistry,
                client
            );

            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> action.doExecute(null, mstr, null));
            // TODO error message probably too verbose
            assertEquals(
                "parts of writeable [SearchRequest{searchType=QUERY_THEN_FETCH, indices=[], "
                    + "indicesOptions=IndicesOptions[ignore_unavailable=false, allow_no_indices=true, expand_wildcards_open=true, "
                    + "expand_wildcards_closed=false, expand_wildcards_hidden=false, allow_aliases_to_multiple_indices=true, "
                    + "forbid_closed_indices=true, ignore_aliases=false, ignore_throttled=true], routing='null', preference='null', "
                    + "requestCache=null, scroll=null, maxConcurrentShardRequests=0, batchedReduceSize=512, preFilterShardSize=null, "
                    + "allowPartialSearchResults=null, localClusterAlias=null, getOrCreateAbsoluteStartMillis=-1, "
                    + "ccsMinimizeRoundtrips=true, source={\"query\":{\"dummy\":{}},\"explain\":false}}] "
                    + "are not compatible with version 8.0.0 and the 'check_ccs_compatibility' is enabled.",
                ex.getMessage()
            );
            assertEquals("This query isn't serializable to nodes on or before 8.0.0", ex.getCause().getMessage());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }
}
