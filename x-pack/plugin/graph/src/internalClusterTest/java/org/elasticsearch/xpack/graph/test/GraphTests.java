/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.graph.test;

import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.protocol.xpack.graph.GraphExploreResponse;
import org.elasticsearch.protocol.xpack.graph.Hop;
import org.elasticsearch.protocol.xpack.graph.Vertex;
import org.elasticsearch.protocol.xpack.graph.VertexRequest;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.core.graph.action.GraphExploreRequestBuilder;
import org.elasticsearch.xpack.graph.Graph;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;

public class GraphTests extends ESSingleNodeTestCase {

    static class DocTemplate {
        int numDocs;
        String[] people;
        String description;
        String decade;

        DocTemplate(int numDocs, String decade, String description, String... people) {
            super();
            this.decade = decade;
            this.numDocs = numDocs;
            this.description = description;
            this.people = people;
        }
    }

    static final DocTemplate[] socialNetTemplate = {
        new DocTemplate(10, "60s", "beatles", "john", "paul", "george", "ringo"),
        new DocTemplate(2, "60s", "collaboration", "ravi", "george"),
        new DocTemplate(3, "80s", "travelling wilburys", "roy", "george", "jeff"),
        new DocTemplate(5, "80s", "travelling wilburys", "roy", "jeff", "bob"),
        new DocTemplate(1, "70s", "collaboration", "roy", "elvis"),
        new DocTemplate(10, "90s", "nirvana", "dave", "kurt"),
        new DocTemplate(2, "00s", "collaboration", "dave", "paul"),
        new DocTemplate(2, "80s", "collaboration", "stevie", "paul"),
        new DocTemplate(2, "70s", "collaboration", "john", "yoko"),
        new DocTemplate(100, "70s", "fillerDoc", "other", "irrelevant", "duplicated", "spammy", "background") };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("type", "decade", "type=keyword", "people", "type=keyword", "description", "type=text,fielddata=true")
        );
        createIndex("idx_unmapped");

        ensureGreen();

        int numDocs = 0;
        for (DocTemplate dt : socialNetTemplate) {
            for (int i = 0; i < dt.numDocs; i++) {
                // Supply a doc ID for deterministic routing of docs to shards
                client().prepareIndex("test", "type", "doc#" + numDocs)
                    .setSource("decade", dt.decade, "people", dt.people, "description", dt.description)
                    .get();
                numDocs++;
            }
        }
        client().admin().indices().prepareRefresh("test").get();
        // Ensure single segment with no deletes. Hopefully solves test instability in
        // issue https://github.com/elastic/x-pack-elasticsearch/issues/918
        ForceMergeResponse actionGet = client().admin()
            .indices()
            .prepareForceMerge("test")
            .setFlush(true)
            .setMaxNumSegments(1)
            .execute()
            .actionGet();
        client().admin().indices().prepareRefresh("test").get();
        assertAllSuccessful(actionGet);
        for (IndexShardSegments seg : client().admin().indices().prepareSegments().get().getIndices().get("test")) {
            ShardSegments[] shards = seg.getShards();
            for (ShardSegments shardSegments : shards) {
                assertEquals(1, shardSegments.getSegments().size());
            }
        }

        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), numDocs);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ScriptedTimeoutPlugin.class, Graph.class, LocalStateCompositeXPackPlugin.class);
    }

    public void testSignificanceQueryCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        grb.createNextHop(null).addVertexRequest("people").size(100).minDocCount(1); // friends of members of beatles

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");
        checkVertexDepth(response, 1, "stevie", "yoko", "roy");
        checkVertexIsMoreImportant(response, "John's only collaboration is more relevant than one of Paul's many", "yoko", "stevie");
        checkVertexIsMoreImportant(response, "John's only collaboration is more relevant than George's with profligate Roy", "yoko", "roy");
        assertNull("Elvis is a 3rd tier connection so should not be returned here", response.getVertex(Vertex.createId("people", "elvis")));
    }

    @Override
    protected Settings nodeSettings() {
        // Disable security otherwise authentication failures happen creating indices.
        Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        newSettings.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        // newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        // newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        // newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        return newSettings.build();
    }

    public void testTargetedQueryCrawl() {
        // Tests use of a client-provided query to steer exploration
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        // 70s friends of beatles
        grb.createNextHop(QueryBuilders.termQuery("decade", "70s")).addVertexRequest("people").size(100).minDocCount(1);

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");
        checkVertexDepth(response, 1, "yoko");
        assertNull("Roy collaborated with George in the 80s not the 70s", response.getVertex(Vertex.createId("people", "roy")));
        assertNull("Stevie collaborated with Paul in the 80s not the 70s", response.getVertex(Vertex.createId("people", "stevie")));

    }

    public void testLargeNumberTermsStartCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        Hop hop1 = grb.createNextHop(null);
        VertexRequest peopleNames = hop1.addVertexRequest("people").minDocCount(1);
        peopleNames.addInclude("john", 1);

        for (int i = 0; i < BooleanQuery.getMaxClauseCount() + 1; i++) {
            peopleNames.addInclude("unknown" + i, 1);
        }

        grb.createNextHop(null).addVertexRequest("people").size(100).minDocCount(1); // friends of members of beatles

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john");
        checkVertexDepth(response, 1, "yoko");
    }

    public void testTargetedQueryCrawlDepth2() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        // 00s friends of beatles
        grb.createNextHop(QueryBuilders.termQuery("decade", "00s")).addVertexRequest("people").size(100).minDocCount(1);
        // 90s friends of friends of beatles
        grb.createNextHop(QueryBuilders.termQuery("decade", "90s")).addVertexRequest("people").size(100).minDocCount(1);

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");
        checkVertexDepth(response, 1, "dave");
        checkVertexDepth(response, 2, "kurt");

    }

    public void testPopularityQueryCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        // Turning off the significance feature means we reward popularity
        grb.useSignificance(false);
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        grb.createNextHop(null).addVertexRequest("people").size(100).minDocCount(1); // friends of members of beatles

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");
        checkVertexDepth(response, 1, "stevie", "yoko", "roy");
        checkVertexIsMoreImportant(response, "Yoko has more collaborations than Stevie", "yoko", "stevie");
        checkVertexIsMoreImportant(response, "Roy has more collaborations than Stevie", "roy", "stevie");
        assertNull("Elvis is a 3rd tier connection so should not be returned here", response.getVertex(Vertex.createId("people", "elvis")));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/55396")
    public void testTimedoutQueryCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        grb.setTimeout(TimeValue.timeValueMillis(400));
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        // 00s friends of beatles
        grb.createNextHop(QueryBuilders.termQuery("decade", "00s")).addVertexRequest("people").size(100).minDocCount(1);
        // A query that should cause a timeout
        ScriptQueryBuilder timeoutQuery = QueryBuilders.scriptQuery(
            new Script(ScriptType.INLINE, "mockscript", "graph_timeout", Collections.emptyMap())
        );
        grb.createNextHop(timeoutQuery).addVertexRequest("people").size(100).minDocCount(1);

        GraphExploreResponse response = grb.get();
        assertTrue(Strings.toString(response), response.isTimedOut());

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");

        // Most of the test runs we reach dave in the allotted time before we hit our
        // intended delay but sometimes this doesn't happen so I commented this line out.

        // checkVertexDepth(response, 1, "dave");
    }

    public void testNonDiversifiedCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");

        Hop hop1 = grb.createNextHop(QueryBuilders.termsQuery("people", "dave", "other"));
        hop1.addVertexRequest("people").size(10).minDocCount(1);

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "dave", "kurt", "other", "spammy");
        checkVertexIsMoreImportant(response, "Due to duplication and no diversification spammy content beats signal", "spammy", "kurt");
    }

    public void testDiversifiedCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        grb.sampleDiversityField("description").maxDocsPerDiversityValue(1);

        Hop hop1 = grb.createNextHop(QueryBuilders.termsQuery("people", "dave", "other"));
        hop1.addVertexRequest("people").size(10).minDocCount(1);

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "dave", "kurt");
        assertNull("Duplicate spam should be removed from the results", response.getVertex(Vertex.createId("people", "spammy")));
    }

    public void testInvalidDiversifiedCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        grb.sampleDiversityField("description").maxDocsPerDiversityValue(1);

        Hop hop1 = grb.createNextHop(QueryBuilders.termsQuery("people", "roy", "other"));
        hop1.addVertexRequest("people").size(10).minDocCount(1);

        Throwable expectedError = null;
        try {
            GraphExploreResponse response = grb.get();
            if (response.getShardFailures().length > 0) {
                expectedError = response.getShardFailures()[0].getCause();
            }
        } catch (Exception rte) {
            expectedError = rte;
        }
        assertNotNull(expectedError);
        String message = expectedError.toString();
        assertTrue(message.contains("Sample diversifying key must be a single valued-field"));
    }

    public void testMappedAndUnmappedQueryCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices(
            "test",
            "idx_unmapped"
        );
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        grb.createNextHop(null).addVertexRequest("people").size(100).minDocCount(1); // friends of members of beatles

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");
        checkVertexDepth(response, 1, "stevie", "yoko", "roy");
        checkVertexIsMoreImportant(response, "John's only collaboration is more relevant than one of Paul's many", "yoko", "stevie");
        checkVertexIsMoreImportant(response, "John's only collaboration is more relevant than George's with profligate Roy", "yoko", "roy");
        assertNull("Elvis is a 3rd tier connection so should not be returned here", response.getVertex(Vertex.createId("people", "elvis")));
    }

    public void testUnmappedQueryCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("idx_unmapped");
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1);

        GraphExploreResponse response = grb.get();
        assertEquals(0, response.getConnections().size());
        assertEquals(0, response.getVertices().size());

    }

    public void testRequestValidation() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");

        try {
            grb.get();
            fail("Validation failure expected");
        } catch (ActionRequestValidationException rte) {
            assertTrue(rte.getMessage().contains(GraphExploreRequest.NO_HOPS_ERROR_MESSAGE));
        }

        grb.createNextHop(null);

        try {
            grb.get();
            fail("Validation failure expected");
        } catch (ActionRequestValidationException rte) {
            assertTrue(rte.getMessage().contains(GraphExploreRequest.NO_VERTICES_ERROR_MESSAGE));
        }

    }

    private static void checkVertexDepth(GraphExploreResponse response, int expectedDepth, String... ids) {
        for (String id : ids) {
            Vertex vertex = response.getVertex(Vertex.createId("people", id));
            assertNotNull("Expected to find " + id, vertex);
            assertEquals(id + " found at wrong hop depth", expectedDepth, vertex.getHopDepth());
        }
    }

    private static void checkVertexIsMoreImportant(GraphExploreResponse response, String why, String strongerId, String weakerId) {
        // *Very* rarely I think the doc delete randomization and background merges conspire to
        // make this test fail. Scores vary slightly due to deletes I suspect.
        Vertex strongVertex = response.getVertex(Vertex.createId("people", strongerId));
        assertNotNull(strongVertex);
        Vertex weakVertex = response.getVertex(Vertex.createId("people", weakerId));
        assertNotNull(weakVertex);
        assertThat(why, strongVertex.getWeight(), greaterThan(weakVertex.getWeight()));
    }

    public static class ScriptedTimeoutPlugin extends MockScriptPlugin {
        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("graph_timeout", params -> {
                try {
                    Thread.sleep(750);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
