package org.elasticsearch.river.couchdb;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.river.couchdb.http.Http;
import org.elasticsearch.river.couchdb.run.Couch;
import org.elasticsearch.search.SearchHits;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.io.FileSystemUtils.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class CouchDBRiverTests {
    private final Http http = new Http();

    private Couch couch;
    private CouchDatabase database;

    @BeforeMethod
    public void setUp() throws Exception {

        removeExistingTestIndexes();

        couch = new Couch(http);
        couch.start();

        database = couch.createDatabase("database");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if ( couch != null ) couch.stop();
    }

    private void removeExistingTestIndexes() {
        deleteRecursively(new File("data"));
    }

    @Test
    public void riverFlowsWhenCouchIsRunning() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local")
                .build();

        Node node = nodeBuilder().settings(settings).node();

        startRiver(node, couchDbRiverFor(this.couch, this.database));

        database.createDocument("id", "{ \"test\" : \"value\" } ");

        Thread.sleep(1000);

        SearchResponse searchResponse = queryNodeFor(node, matchAllQuery());

        SearchHits hits = searchResponse.getHits();
        assertThat("Should find one document", hits.getTotalHits(), equalTo(1l));

    }

    private SearchResponse queryNodeFor(Node node, XContentQueryBuilder query) {
        return node.client().search(searchRequest()
                .source(searchSource().query(query))).actionGet();
    }

    private XContentBuilder couchDbRiverFor(Couch couch, CouchDatabase database) throws IOException {
        return jsonBuilder()
                .startObject()
                .field("type", "couchdb")
                .field("couchdb" )
                .startObject()
                .field("host", couch.uri().getHost())
                .field("port", couch.uri().getPort())
                .field("db", database.name())
                .endObject()
                .endObject();
    }

    private void startRiver(Node node, XContentBuilder startRiverRequest) {
        node.client().prepareIndex("_river", "db", "_meta").setSource(startRiverRequest).execute().actionGet();
    }
}
