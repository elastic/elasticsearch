/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.integration.EsqlResolveViewRemoteClusterClientRoleIT;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.enrich.EnrichPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.plugin.RemoteEnrichIndexRewriter;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.elasticsearch.test.SecuritySettingsSource.ES_TEST_ROOT_USER;
import static org.elasticsearch.test.SecuritySettingsSource.TEST_USER_NAME;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Plays the part of a querying cluster that rewrites the enrich index in the plan it sends to a remote cluster, with security
 * enabled on both clusters, and checks that the remote cluster only runs the enrich lookup against the index of one of its own
 * enrich policies. The querying user may only read {@code events} on the remote cluster and use its enrich policies.
 */
public class EsqlRemoteEnrichIndexSecurityIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "c1";
    private static final String POLICY_NAME = "hosts";
    private static final String HOST = "192.168.1.2";
    private static final String SECRET = "TOP-SECRET";
    private static final String QUERY = "FROM " + REMOTE_CLUSTER + ":events | ENRICH _remote:hosts ON host WITH os | KEEP host, os";
    private static final String NO_POLICY_INDEX = ".enrich-unknown-1700000000000";
    private static final String UNKNOWN_POLICY_INDEX = ".enrich-other-1700000000000";

    @Nullable
    private String migratedPolicyIndex;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        // EsqlPlugin reads the EncryptionService that TestEncryptionServicePlugin registers, so the latter must come first.
        return List.of(
            TestEncryptionServicePlugin.class,
            EsqlResolveViewRemoteClusterClientRoleIT.EsqlWithTrialLicensePlugin.class,
            IngestCommonPlugin.class
        );
    }

    /**
     * Without an origin the framework's cluster clients run as {@code _system}, which may not wipe the clusters between tests.
     * The test itself authenticates explicitly, as the test user or via {@link #rootClient}.
     */
    @Override
    protected String internalClientOrigin() {
        return MONITORING_ORIGIN;
    }

    @Override
    protected NodeConfigurationSource nodeConfigurationSource() {
        return new EnrichSecuritySettingsSource(createTempDir());
    }

    @Before
    public void setupRemoteIndices() {
        Client remote = rootClient(REMOTE_CLUSTER);
        createIndex(remote, "hosts", "ip", "os");
        index(remote, "hosts", "ip", HOST, "os", "Windows");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("hosts"), "ip", List.of("os"));
        remote.execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME, policy))
            .actionGet();
        remote.execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME))
            .actionGet();

        createIndex(remote, "events", "host");
        index(remote, "events", "host", HOST);
        for (String index : List.of("secrets", NO_POLICY_INDEX)) {
            createIndex(remote, index, "ip", "os");
            index(remote, index, "ip", HOST, "os", SECRET);
        }
        assertAcked(
            remote.admin()
                .indices()
                .prepareCreate(UNKNOWN_POLICY_INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping(
                    Map.of(
                        "_meta",
                        Map.of(EnrichPolicy.ENRICH_POLICY_NAME_FIELD_NAME, "other"),
                        "properties",
                        Map.of("ip", Map.of("type", "keyword"), "os", Map.of("type", "keyword"))
                    )
                )
        );
        index(remote, UNKNOWN_POLICY_INDEX, "ip", HOST, "os", SECRET);
    }

    @After
    public void cleanup() {
        for (TransportService ts : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
            ((MockTransportService) ts).clearAllRules();
        }
        Client remote = rootClient(REMOTE_CLUSTER);
        try {
            remote.execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME))
                .actionGet();
        } catch (ResourceNotFoundException e) {
            // setupRemoteIndices failed before creating the policy; don't hide that failure
        }
        assertAcked(
            remote.admin()
                .indices()
                .prepareDelete(
                    Stream.of("hosts", "events", "secrets", NO_POLICY_INDEX, UNKNOWN_POLICY_INDEX, migratedPolicyIndex)
                        .filter(Objects::nonNull)
                        .toArray(String[]::new)
                )
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
        );
        // The test cluster wipe skips system indices, so drop the security index created by the system index test.
        Client security = new OriginSettingClient(cluster(REMOTE_CLUSTER).client(), SECURITY_ORIGIN);
        assertAcked(security.admin().indices().prepareDelete(SECURITY_MAIN_ALIAS).setIndicesOptions(IndicesOptions.lenientExpandOpen()));
    }

    public void testUnmodifiedRemotePlanEnrichesFromPolicyIndex() {
        try (EsqlQueryResponse resp = runQueryAsTestUser()) {
            assertThat(rows(resp), equalTo(List.of(List.of(HOST, "Windows"))));
        }
    }

    public void testUnmodifiedRemotePlanEnrichesFromMigratedPolicyIndex() {
        migratedPolicyIndex = migratePolicyIndex();
        try (EsqlQueryResponse resp = runQueryAsTestUser()) {
            assertThat(rows(resp), equalTo(List.of(List.of(HOST, "Windows"))));
        }
    }

    public void testRemotePlanTargetingRegularIndexIsRejected() {
        assertTamperedLookupRejected("secrets");
    }

    public void testRemotePlanTargetingSystemIndexIsRejected() {
        Client remote = rootClient(REMOTE_CLUSTER);
        // The security system index is only created once something is stored in it, such as a native role.
        new PutRoleRequestBuilder(remote).name("unused").cluster("monitor").get();
        String securityIndex = cluster(REMOTE_CLUSTER).clusterService()
            .state()
            .metadata()
            .getProject()
            .getIndicesLookup()
            .get(SECURITY_MAIN_ALIAS)
            .getIndices()
            .getFirst()
            .getName();
        assertTamperedLookupRejected(securityIndex);
    }

    public void testRemotePlanTargetingEnrichIndexWithoutPolicyIsRejected() {
        ElasticsearchSecurityException e = assertTamperedLookupRejected(NO_POLICY_INDEX);
        assertThat(e.getMessage(), containsString("enrich lookup on index [" + NO_POLICY_INDEX + "] is not allowed"));
    }

    public void testRemotePlanTargetingEnrichIndexOfUnknownPolicyIsRejected() {
        ElasticsearchSecurityException e = assertTamperedLookupRejected(UNKNOWN_POLICY_INDEX);
        assertThat(e.getMessage(), containsString("enrich lookup on index [" + UNKNOWN_POLICY_INDEX + "] is not allowed"));
    }

    private ElasticsearchSecurityException assertTamperedLookupRejected(String index) {
        AtomicInteger rewrittenRequests = RemoteEnrichIndexRewriter.rewriteRemoteEnrichIndex(
            cluster(LOCAL_CLUSTER).getInstances(TransportService.class),
            REMOTE_CLUSTER,
            index
        );
        Exception e = expectThrows(Exception.class, () -> {
            try (EsqlQueryResponse resp = runQueryAsTestUser()) {
                fail("ENRICH on the remote cluster read [" + index + "] and returned " + rows(resp));
            }
        });
        assertThat(rewrittenRequests.get(), greaterThan(0));
        ElasticsearchSecurityException securityException = (ElasticsearchSecurityException) ExceptionsHelper.unwrap(
            e,
            ElasticsearchSecurityException.class
        );
        assertThat(securityException, notNullValue());
        assertThat(securityException.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(securityException.getMessage(), containsString("[" + index + "]"));
        return securityException;
    }

    /**
     * Does to the policy's enrich index what the system index migration does to it: copies it into an index named with
     * {@link SystemIndices#UPGRADED_INDEX_SUFFIX}, then deletes the original and points its name and aliases at the copy.
     */
    private String migratePolicyIndex() {
        Client remote = rootClient(REMOTE_CLUSTER);
        ProjectMetadata project = cluster(REMOTE_CLUSTER).clusterService().state().metadata().getProject();
        IndexMetadata current = project.index(
            project.getIndicesLookup().get(EnrichPolicy.getBaseName(POLICY_NAME)).getIndices().getFirst()
        );
        String currentName = current.getIndex().getName();
        String migrated = currentName + SystemIndices.UPGRADED_INDEX_SUFFIX;
        assertAcked(
            remote.admin()
                .indices()
                .prepareCreate(migrated)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.hidden", true))
                .setMapping(current.mapping().getSourceAsMap())
        );
        new ReindexRequestBuilder(remote).source(currentName).destination(migrated).refresh(true).get();
        assertAcked(
            remote.admin()
                .indices()
                .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .removeIndex(currentName)
                .addAlias(migrated, currentName)
                .addAlias(migrated, EnrichPolicy.getBaseName(POLICY_NAME))
        );
        return migrated;
    }

    private EsqlQueryResponse runQueryAsTestUser() {
        Client client = cluster(LOCAL_CLUSTER).client()
            .filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING)));
        return client.execute(EsqlQueryAction.INSTANCE, EsqlQueryRequest.syncEsqlQueryRequest(QUERY)).actionGet(30, TimeUnit.SECONDS);
    }

    private static List<List<Object>> rows(EsqlQueryResponse resp) {
        List<List<Object>> rows = new ArrayList<>();
        for (Iterable<Object> row : resp.rows()) {
            List<Object> values = new ArrayList<>();
            row.forEach(values::add);
            rows.add(values);
        }
        return rows;
    }

    private Client rootClient(String clusterAlias) {
        return cluster(clusterAlias).client()
            .filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING)));
    }

    private static void createIndex(Client client, String index, String... keywordFields) {
        List<String> mapping = new ArrayList<>();
        for (String field : keywordFields) {
            mapping.add(field);
            mapping.add("type=keyword");
        }
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping(mapping.toArray(String[]::new))
        );
    }

    private static void index(Client client, String index, Object... source) {
        client.prepareIndex(index).setSource(source).get();
        client.admin().indices().prepareRefresh(index).get();
    }

    /**
     * Composite x-pack plugin with enrich added, since a node can only register one {@code LocalStateCompositeXPackPlugin}.
     */
    public static class LocalStateSecurityWithEnrich extends LocalStateSecurity {
        public LocalStateSecurityWithEnrich(Settings settings, Path configPath) throws Exception {
            super(settings, configPath);
            LocalStateSecurityWithEnrich thisVar = this;
            plugins.add(new EnrichPlugin(settings) {
                @Override
                protected XPackLicenseState getLicenseState() {
                    return thisVar.getLicenseState();
                }
            });
        }
    }

    private static class EnrichSecuritySettingsSource extends SecuritySettingsSource {
        private static final String TEST_ROLE_YML = """
            user:
              cluster: [ monitor_enrich ]
              indices:
                - names: [ 'events' ]
                  privileges: [ read, read_cross_cluster, view_index_metadata ]
            """;

        private EnrichSecuritySettingsSource(Path parentFolder) {
            super(false, parentFolder, ESIntegTestCase.Scope.SUITE);
        }

        @Override
        protected String configRoles() {
            return TEST_ROLE_YML + "\n" + SecuritySettingsSourceField.ES_TEST_ROOT_ROLE_YML;
        }

        @Override
        public Collection<Class<? extends Plugin>> nodePlugins() {
            List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
            plugins.replaceAll(p -> p == LocalStateSecurity.class ? LocalStateSecurityWithEnrich.class : p);
            return plugins;
        }
    }
}
