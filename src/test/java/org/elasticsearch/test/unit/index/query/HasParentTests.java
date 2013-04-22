package org.elasticsearch.test.unit.index.query;

import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.HasParentQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.Test;

import java.util.Map;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.deleteByQueryRequest;
import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Test
public class HasParentTests extends AbstractNodesTests {

    @Test
    public void testHasParent() throws Exception {
        startNode("node1");
        client("node1").admin().indices().create(createIndexRequest("index")).actionGet();
        client("node1").admin().indices().putMapping(putMappingRequest("index").type("parent").source(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("parent")
                .endObject()
                .endObject()
        )).actionGet();
        client("node1").admin().indices().putMapping(putMappingRequest("index").type("child").source(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("child")
                        .startObject("_parent")
                            .field("type", "parent")
                        .endObject()
                    .endObject()
                .endObject()
        )).actionGet();
        DeleteByQueryResponse response = client("node1").deleteByQuery(deleteByQueryRequest("index").query(new HasParentQueryBuilder("parent", new MatchAllQueryBuilder()))).actionGet();
        assertThat(response, notNullValue());
        for (Map.Entry<String,IndexDeleteByQueryResponse> entry : response.getIndices().entrySet()) {
            IndexDeleteByQueryResponse subresp = entry.getValue();
            assertThat(subresp.getFailedShards(), equalTo(0));
        }
    }

}
