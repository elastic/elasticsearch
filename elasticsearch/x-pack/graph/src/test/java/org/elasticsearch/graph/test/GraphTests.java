/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph.test;

import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.graph.action.GraphExploreAction;
import org.elasticsearch.graph.action.GraphExploreRequest;
import org.elasticsearch.graph.action.GraphExploreRequestBuilder;
import org.elasticsearch.graph.action.GraphExploreResponse;
import org.elasticsearch.graph.action.Hop;
import org.elasticsearch.graph.action.Vertex;
import org.elasticsearch.graph.action.VertexRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.marvel.Monitoring;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.shield.Security;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.watcher.Watcher;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;


public class GraphTests extends ESSingleNodeTestCase {
    
    static class DocTemplate {
        int numDocs;
        String[] people;
        String description;
        String decade;

        public DocTemplate(int numDocs, String decade, String description, String... people) {
            super();
            this.decade = decade;
            this.numDocs = numDocs;
            this.description = description;
            this.people = people;
        }
    }
    

    static final DocTemplate[] socialNetTemplate = {
        new DocTemplate(10,  "60s", "beatles", "john", "paul", "george", "ringo"),
        new DocTemplate(2,   "60s", "collaboration", "ravi", "george"), 
        new DocTemplate(3,   "80s", "travelling wilburys", "roy", "george", "jeff"), 
        new DocTemplate(5,   "80s", "travelling wilburys", "roy", "jeff", "bob"), 
        new DocTemplate(1,   "70s", "collaboration", "roy", "elvis"), 
        new DocTemplate(10,  "90s", "nirvana", "dave", "kurt"), 
        new DocTemplate(2,   "00s", "collaboration", "dave", "paul"),     
        new DocTemplate(2,   "80s", "collaboration", "stevie", "paul"),     
        new DocTemplate(2,   "70s", "collaboration", "john", "yoko"),     
        new DocTemplate(100, "70s", "fillerDoc", "other", "irrelevant", "duplicated", "spammy", "background")
    };   
    

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 2, SETTING_NUMBER_OF_REPLICAS, 0)
                .addMapping("type",
                        "decade", "type=keyword",
                        "people", "type=keyword",
                        "description", "type=text,fielddata=true"));
        createIndex("idx_unmapped");

        ensureGreen();

        int numDocs = 0;
        for (DocTemplate dt : socialNetTemplate) {
            for (int i = 0; i < dt.numDocs; i++) {
                client().prepareIndex("test", "type").setSource("decade", dt.decade, "people", dt.people, "description", dt.description)
                        .get();
                numDocs++;
            }
        }
        client().admin().indices().prepareRefresh("test").get();
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), numDocs);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ScriptedTimeoutPlugin.class, XPackPlugin.class);
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
        assertNull("Elvis is a 3rd tier connection so should not be returned here", response.getVertex(Vertex.createId("people","elvis")));
    }
    
    
    @Override
    public Settings nodeSettings()  {
        // Disable Shield otherwise authentication failures happen creating indices. 
        Builder newSettings = Settings.builder();
        newSettings.put(XPackPlugin.featureEnabledSetting(Security.NAME), false);
        newSettings.put(XPackPlugin.featureEnabledSetting(Monitoring.NAME), false);
        newSettings.put(XPackPlugin.featureEnabledSetting(Watcher.NAME), false);          
        return newSettings.build();
    }

    public void testTargetedQueryCrawl() {
        // Tests use of a client-provided query to steer exploration
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        //70s friends of beatles
        grb.createNextHop(QueryBuilders.termQuery("decade", "70s")).addVertexRequest("people").size(100).minDocCount(1); 

        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");
        checkVertexDepth(response, 1, "yoko");
        assertNull("Roy collaborated with George in the 80s not the 70s", response.getVertex(Vertex.createId("people","roy")));
        assertNull("Stevie collaborated with Paul in the 80s not the 70s", response.getVertex(Vertex.createId("people","stevie")));
        
    }
    
    

    public void testLargeNumberTermsStartCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        Hop hop1 = grb.createNextHop(null);
        VertexRequest peopleNames = hop1.addVertexRequest("people").minDocCount(1); 
        peopleNames.addInclude("john", 1);
        
        for (int i = 0; i < BooleanQuery.getMaxClauseCount()+1; i++) {
            peopleNames.addInclude("unknown"+i, 1);            
        }
        
        grb.createNextHop(null).addVertexRequest("people").size(100).minDocCount(1); // friends of members of beatles
        
        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john");
        checkVertexDepth(response, 1,  "yoko");
    }    

    public void testTargetedQueryCrawlDepth2() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        //00s friends of beatles
        grb.createNextHop(QueryBuilders.termQuery("decade", "00s")).addVertexRequest("people").size(100).minDocCount(1); 
        //90s friends of friends of beatles
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
        assertNull("Elvis is a 3rd tier connection so should not be returned here", response.getVertex(Vertex.createId("people","elvis")));
    }    
    
    public void testTimedoutQueryCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE).setIndices("test");
        grb.setTimeout(TimeValue.timeValueMillis(400));
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        //00s friends of beatles
        grb.createNextHop(QueryBuilders.termQuery("decade", "00s")).addVertexRequest("people").size(100).minDocCount(1); 
        // A query that should cause a timeout
        ScriptQueryBuilder timeoutQuery = QueryBuilders.scriptQuery(new Script(NativeTestScriptedTimeout.TEST_NATIVE_SCRIPT_TIMEOUT,
                ScriptType.INLINE, "native", null));
        grb.createNextHop(timeoutQuery).addVertexRequest("people").size(100).minDocCount(1);

        GraphExploreResponse response = grb.get();
        assertTrue(response.isTimedOut());

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");
        
        // Most of the test runs we reach dave in the allotted time before we hit our 
        // intended delay but sometimes this doesn't happen so I commented this line out.
        
        // checkVertexDepth(response, 1, "dave");

        // This is the point where we should certainly have run out of time due
        // to the test query plugin with a deliberate pause
        assertNull("Should have timed out trying to crawl out to kurt", response.getVertex(Vertex.createId("people","kurt")));
        
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
        assertNull("Duplicate spam should be removed from the results", response.getVertex(Vertex.createId("people","spammy")));
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
                throw ((ShardSearchFailure) response.getShardFailures()[0]).getCause();
            }
        } catch (Throwable rte) {
            expectedError = rte;

        }
        assertNotNull(expectedError);
        String message = expectedError.toString();
        assertTrue(message.contains("Sample diversifying key must be a single valued-field"));
    }
    
    public void testMappedAndUnmappedQueryCrawl() {
        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client(), GraphExploreAction.INSTANCE)
                .setIndices("test", "idx_unmapped");
        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("description", "beatles"));
        hop1.addVertexRequest("people").size(10).minDocCount(1); // members of beatles
        grb.createNextHop(null).addVertexRequest("people").size(100).minDocCount(1); // friends of members of beatles
        
        GraphExploreResponse response = grb.get();

        checkVertexDepth(response, 0, "john", "paul", "george", "ringo");
        checkVertexDepth(response, 1, "stevie", "yoko", "roy");
        checkVertexIsMoreImportant(response, "John's only collaboration is more relevant than one of Paul's many", "yoko", "stevie");
        checkVertexIsMoreImportant(response, "John's only collaboration is more relevant than George's with profligate Roy", "yoko", "roy");
        assertNull("Elvis is a 3rd tier connection so should not be returned here", response.getVertex(Vertex.createId("people","elvis")));
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

        Hop hop = grb.createNextHop(null);

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
        Vertex weakVertex = response.getVertex(Vertex.createId("people",weakerId));
        assertNotNull(weakVertex);
        assertThat(why, strongVertex.getWeight(), greaterThan(weakVertex.getWeight()));
    }

    public static class ScriptedTimeoutPlugin extends Plugin {
        @Override
        public String name() {
            return "test-scripted-graph-timeout";
        }

        @Override
        public String description() {
            return "Test for scripted timeouts on graph searches";
        }

        public void onModule(ScriptModule module) {
            module.registerScript(NativeTestScriptedTimeout.TEST_NATIVE_SCRIPT_TIMEOUT, NativeTestScriptedTimeout.Factory.class);
        }
    }

    public static class NativeTestScriptedTimeout extends AbstractSearchScript {

        public static final String TEST_NATIVE_SCRIPT_TIMEOUT = "native_test_graph_timeout_script";

        public static class Factory implements NativeScriptFactory {

            @Override
            public ExecutableScript newScript(Map<String, Object> params) {
                return new NativeTestScriptedTimeout();
            }

            @Override
            public boolean needsScores() {
                return false;
            }
        }

        @Override
        public Object run() {
            try {
                Thread.sleep(750);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }

}