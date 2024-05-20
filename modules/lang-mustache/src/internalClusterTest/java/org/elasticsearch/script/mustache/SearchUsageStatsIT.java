/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.admin.cluster.stats.SearchUsageStats;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SearchUsageStatsIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MustachePlugin.class);
    }

    public void testSearchUsageStats() throws IOException {
        {
            SearchUsageStats stats = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSearchUsageStats();
            assertEquals(0, stats.getTotalSearchCount());
            assertEquals(0, stats.getQueryUsage().size());
            assertEquals(0, stats.getSectionsUsage().size());
        }

        {
            Request request = new Request("GET", "/_search/template");
            request.setJsonEntity("""
                {
                  "source": {
                    "query": {
                      "{{query_type}}": {
                        "message" : "{{query_string}}"
                      }
                    }
                  },
                  "params": {
                    "query_string": "text query",
                    "query_type" : "match"
                  }
                }
                """);
            getRestClient().performRequest(request);
        }
        {
            assertAcked(clusterAdmin().preparePutStoredScript().setId("testTemplate").setContent(new BytesArray("""
                {
                  "script": {
                    "lang": "mustache",
                    "source": {
                      "query": {
                        "match": {
                          "theField": "{{fieldParam}}"
                        }
                      }
                    }
                  }
                }"""), XContentType.JSON));
            Request request = new Request("GET", "/_search/template");
            request.setJsonEntity("""
                {
                  "id": "testTemplate",
                  "params": {
                    "fieldParam": "text query"
                  }
                }
                """);
            getRestClient().performRequest(request);
        }
        {
            Request request = new Request("POST", "/_msearch/template");
            request.setJsonEntity("""
                {}
                {"id": "testTemplate", "params": {"fieldParam":"text query"}}
                {}
                {"source": {"query": { "match": {"message" : "{{query_string}}"}}},"params": {"query_string": "text query"}}
                """);
            getRestClient().performRequest(request);
        }

        SearchUsageStats stats = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSearchUsageStats();
        assertEquals(4, stats.getTotalSearchCount());
        assertEquals(1, stats.getQueryUsage().size());
        assertEquals(4, stats.getQueryUsage().get("match").longValue());
        assertEquals(1, stats.getSectionsUsage().size());
        assertEquals(4, stats.getSectionsUsage().get("query").longValue());
    }
}
