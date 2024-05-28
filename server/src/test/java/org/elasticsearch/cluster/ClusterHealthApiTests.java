package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit test(s) for Cluster Health
 */
public class ClusterHealthApiTests extends ESIntegTestCase {

    /**
     * Test the basic functionality of the Cluster Health API.
     * Verifies that the cluster health response is not null and contains essential information.
     */
    @Test
    public void testClusterHealthApiBasicFunctionality() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse response = client().admin().cluster().health(request).actionGet();

        assertNotNull(response);
        assertThat(response.getStatus(), notNullValue());

        assertThat(response.getActiveShards(), greaterThanOrEqualTo(0));
        assertThat(response.getActivePrimaryShards(), greaterThanOrEqualTo(0));
        assertThat(response.getInitializingShards(), greaterThanOrEqualTo(0));
        assertThat(response.getRelocatingShards(), greaterThanOrEqualTo(0));
        assertThat(response.getUnassignedShards(), greaterThanOrEqualTo(0));
        assertThat(response.getStatus(), notNullValue());
        assertThat(response.getStatus(),
            anyOf(equalTo(ClusterHealthStatus.GREEN), equalTo(ClusterHealthStatus.YELLOW), equalTo(ClusterHealthStatus.RED)));
    }

    /**
     * Test the Cluster Health API with specific indices.
     * Verifies that the cluster health response is not null when querying with specific indices.
     */
    @Test
    public void testClusterHealthWithIndices() {
        String[] indices = {"index1", "index2"};
        ClusterHealthRequest request = new ClusterHealthRequest(indices);
        ClusterHealthResponse response = client().admin().cluster().health(request).actionGet();

        assertNotNull(response);
        assertThat(response.getStatus(), notNullValue());
    }

    /**
     * Test the Cluster Health API with a timeout.
     * Verifies that the cluster health response is not null when specifying a timeout.
     */
    @Test
    public void testClusterHealthWithTimeout() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.timeout("10s");
        ClusterHealthResponse response = client().admin().cluster().health(request).actionGet();

        assertNotNull(response);
        assertThat(response.getStatus(), notNullValue());
    }

    /**
     * Test the Cluster Health API with custom parameters.
     * Verifies that the cluster health response status is YELLOW when waiting for YELLOW status.
     */
    @Test
    public void testClusterHealthWithCustomParameters() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.waitForStatus(ClusterHealthStatus.YELLOW);
        request.waitForEvents(Priority.NORMAL);
        ClusterHealthResponse response = client().admin().cluster().health(request).actionGet();

        assertNotNull(response);
        assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
    }
}
