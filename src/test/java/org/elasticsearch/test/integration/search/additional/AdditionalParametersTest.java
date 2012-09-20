package org.elasticsearch.test.integration.search.additional;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AdditionalParametersTest extends AbstractNodesTests {
  private Client client;

  @BeforeClass
  public void createNodes() throws Exception {
      startNode("server1");
      client = client("server1");
  }

  @AfterClass
  public void closeNodes() {
      client.close();
      closeAllNodes();
  }
  
  @Test
  public void testPassAdditionalParameters() throws Exception {
      try {
          client.admin().indices().prepareDelete("test").execute().actionGet();
      } catch (Exception e) {
          // ignore
      }
      client.admin().indices().prepareCreate("test").execute().actionGet();
      client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

      client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
              .field("id", "1")
              .field("name", "aaa")
              .endObject()).execute().actionGet();

      client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

      SearchResponse searchResponse = client.prepareSearch()
              .setQuery(matchAllQuery())
              .addAdditionalParameter("userId", "12345")
              .addAdditionalParameter("token", "dksajas812enxaskhas")
              .execute().actionGet();

      assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
  }
}
