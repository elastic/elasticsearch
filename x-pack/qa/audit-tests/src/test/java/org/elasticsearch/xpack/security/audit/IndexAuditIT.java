/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class IndexAuditIT extends ESIntegTestCase {
    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        TestCluster testCluster = super.buildTestCluster(scope, seed);
        return new TestCluster(seed) {

            @Override
            public void afterTest() throws IOException {
                testCluster.afterTest();
            }

            @Override
            public Client client() {
                return testCluster.client();
            }

            @Override
            public int size() {
                return testCluster.size();
            }

            @Override
            public int numDataNodes() {
                return testCluster.numDataNodes();
            }

            @Override
            public int numDataAndMasterNodes() {
                return testCluster.numDataAndMasterNodes();
            }

            @Override
            public InetSocketAddress[] httpAddresses() {
                return testCluster.httpAddresses();
            }

            @Override
            public void close() throws IOException {
                testCluster.close();
            }

            @Override
            public void ensureEstimatedStats() {
                // stats are not going to be accurate for these tests since the index audit trail
                // is running and changing the values so we wrap the test cluster to skip these
                // checks
            }

            @Override
            public String getClusterName() {
                return testCluster.getClusterName();
            }

            @Override
            public Iterable<Client> getClients() {
                return testCluster.getClients();
            }

            @Override
            public NamedWriteableRegistry getNamedWriteableRegistry() {
                return testCluster.getNamedWriteableRegistry();
            }
        };
    }

    public void testIndexAuditTrailWorking() throws Exception {
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                UsernamePasswordToken.basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray())));
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);
        final AtomicReference<ClusterState> lastClusterState = new AtomicReference<>();
        final boolean found = awaitSecurityAuditIndex(lastClusterState, QueryBuilders.matchQuery("principal", USER));

        assertTrue("Did not find security audit index. Current cluster state:\n" + lastClusterState.get().toString(), found);

        SearchResponse searchResponse = client().prepareSearch(".security_audit_log*").setQuery(
                QueryBuilders.matchQuery("principal", USER)).get();
        assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
        assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("principal"), is(USER));
    }

    public void testAuditTrailTemplateIsRecreatedAfterDelete() throws Exception {
        // this is already "tested" by the test framework since we wipe the templates before and after,
        // but lets be explicit about the behavior
        awaitIndexTemplateCreation();

        // delete the template
        AcknowledgedResponse deleteResponse = client().admin().indices()
                .prepareDeleteTemplate(IndexAuditTrail.INDEX_TEMPLATE_NAME).execute().actionGet();
        assertThat(deleteResponse.isAcknowledged(), is(true));
        awaitIndexTemplateCreation();
    }

    public void testOpaqueIdWorking() throws Exception {
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(Task.X_OPAQUE_ID, "foo");
        options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
            UsernamePasswordToken.basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray())));
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        final AtomicReference<ClusterState> lastClusterState = new AtomicReference<>();
        final boolean found = awaitSecurityAuditIndex(lastClusterState, QueryBuilders.matchQuery("opaque_id", "foo"));

        assertTrue("Did not find security audit index. Current cluster state:\n" + lastClusterState.get().toString(), found);

        SearchResponse searchResponse = client().prepareSearch(".security_audit_log*").setQuery(
            QueryBuilders.matchQuery("opaque_id", "foo")).get();
        assertThat(searchResponse.getHits().getHits().length, greaterThan(0));

        assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("opaque_id"), is("foo"));
    }

    private boolean awaitSecurityAuditIndex(AtomicReference<ClusterState> lastClusterState,
                                            QueryBuilder query) throws InterruptedException {
        final AtomicBoolean indexExists = new AtomicBoolean(false);
        return awaitBusy(() -> {
            if (indexExists.get() == false) {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                lastClusterState.set(state);
                for (ObjectCursor<String> cursor : state.getMetaData().getIndices().keys()) {
                    if (cursor.value.startsWith(".security_audit_log")) {
                        logger.info("found audit index [{}]", cursor.value);
                        indexExists.set(true);
                        break;
                    }
                }

                if (indexExists.get() == false) {
                    return false;
                }
            }

            ensureYellowAndNoInitializingShards(".security_audit_log*");
            logger.info("security audit log index is yellow");
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            lastClusterState.set(state);

            logger.info("refreshing audit indices");
            client().admin().indices().prepareRefresh(".security_audit_log*").get();
            logger.info("refreshed audit indices");
            return client().prepareSearch(".security_audit_log*").setQuery(query)
                .get().getHits().getTotalHits().value > 0;
        }, 60L, TimeUnit.SECONDS);
    }

    private void awaitIndexTemplateCreation() throws InterruptedException {
        boolean found = awaitBusy(() -> {
            GetIndexTemplatesResponse response = client().admin().indices()
                    .prepareGetTemplates(IndexAuditTrail.INDEX_TEMPLATE_NAME).execute().actionGet();
            if (response.getIndexTemplates().size() > 0) {
                for (IndexTemplateMetaData indexTemplateMetaData : response.getIndexTemplates()) {
                    if (IndexAuditTrail.INDEX_TEMPLATE_NAME.equals(indexTemplateMetaData.name())) {
                        return true;
                    }
                }
            }
            return false;
        });

        assertThat("index template [" + IndexAuditTrail.INDEX_TEMPLATE_NAME + "] was not created", found, is(true));
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                .put(SecurityField.USER_SETTING.getKey(), USER + ":" + PASS)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, "security4")
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(XPackClientPlugin.class);
    }

}
