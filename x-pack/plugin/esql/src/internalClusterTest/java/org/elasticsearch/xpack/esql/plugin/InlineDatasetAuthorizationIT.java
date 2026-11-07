/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * A dataset defined where it is read names one object, the data source, and both privileges that govern that object
 * have to hold. These tests differ between users in exactly one of the two at a time: a caller with the index
 * {@code read} on the name but nothing under {@code global.data_source} is refused, and so is a caller with the
 * datasource grant but no index privilege on the name — so neither privilege can be the one doing all the work.
 * <p>
 * Two data nodes, so the node-side gate runs on a node that did not plan the query rather than on the coordinator
 * alone.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class InlineDatasetAuthorizationIT extends SecurityIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final SecureString PASSWORD = new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray());

    private static final String DATA_SOURCE = "inline_ds";

    /** Index {@code read} on the name and {@code read} on the data source: the only user who may read this way. */
    private static final String FULL = "inline_full";
    /** Index {@code read} on the name, nothing under {@code global.data_source}. */
    private static final String NO_DATASOURCE = "inline_no_datasource";
    /** {@code read} on the data source, index {@code read} on another name only. */
    private static final String NO_NAME = "inline_no_name";
    /** Both privileges, but the index grant carries a DLS query an external read cannot apply. */
    private static final String DLS = "inline_dls";

    /** Three one-row files, so a read of the glob has splits to spread over both data nodes. */
    private static Path fixtureDir;

    @Override
    protected String configUsers() {
        StringBuilder users = new StringBuilder(super.configUsers());
        for (String user : List.of(FULL, NO_DATASOURCE, NO_NAME, DLS)) {
            users.append(user).append(':').append(SecuritySettingsSource.TEST_PASSWORD_HASHED).append('\n');
        }
        return users.toString();
    }

    @Override
    protected String configUsersRoles() {
        StringBuilder usersRoles = new StringBuilder(super.configUsersRoles());
        for (String user : List.of(FULL, NO_DATASOURCE, NO_NAME, DLS)) {
            usersRoles.append(user).append(':').append(user).append('\n');
        }
        return usersRoles.toString();
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + """
            inline_full:
              indices:
                - names: [ 'inline_ds' ]
                  privileges: [ 'read' ]
              global:
                data_source:
                  - names: [ 'inline_ds' ]
                    privileges: [ 'read' ]
            inline_no_datasource:
              indices:
                - names: [ 'inline_ds' ]
                  privileges: [ 'read' ]
            inline_no_name:
              indices:
                - names: [ 'inline_other' ]
                  privileges: [ 'read' ]
              global:
                data_source:
                  - names: [ 'inline_ds' ]
                    privileges: [ 'read' ]
            inline_dls:
              indices:
                - names: [ 'inline_ds' ]
                  privileges: [ 'read' ]
                  query: '{"match_all": {}}'
              global:
                data_source:
                  - names: [ 'inline_ds' ]
                    privileges: [ 'read' ]
            """;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // The same wiring as AbstractExternalDataSourceIT, on top of the security plugins the base class installs.
        plugins.add(TestEncryptionServicePlugin.class);
        plugins.add(AbstractExternalDataSourceIT.EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(AbstractExternalDataSourceIT.TestDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.getKey(), false)
            .put(Federation.FEDERATION_ENABLED.getKey(), true)
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    @Before
    public void setupFixtures() throws Exception {
        assumeTrue("requires the table function syntax", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        if (fixtureDir == null) {
            Path dir = createTempDir();
            Files.writeString(dir.resolve("a.csv"), "a\na1\n", StandardCharsets.UTF_8);
            Files.writeString(dir.resolve("b.csv"), "a\na2\n", StandardCharsets.UTF_8);
            Files.writeString(dir.resolve("c.csv"), "a\na3\n", StandardCharsets.UTF_8);
            fixtureDir = dir;
        }
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, DATA_SOURCE, "test", null, new HashMap<>())
            )
        );
    }

    public void testBothPrivilegesTogetherRead() {
        try (EsqlQueryResponse response = query(FULL)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("a1"), List.of("a2"), List.of("a3"))));
        }
    }

    public void testTheDataSourcePrivilegeIsRequired() {
        Exception e = expectThrows(Exception.class, () -> query(NO_DATASOURCE).close());
        assertThat(
            ExceptionsHelper.unwrapCause(e).getMessage(),
            containsString("action [" + EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME + "] is unauthorized")
        );
    }

    public void testTheNamePrivilegeIsRequired() {
        Exception e = expectThrows(Exception.class, () -> query(NO_NAME).close());
        assertThat(
            ExceptionsHelper.unwrapCause(e).getMessage(),
            containsString("action [" + EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME + "] is unauthorized")
        );
    }

    /**
     * A grant that narrows documents or fields cannot be honoured by an external read, so it is refused rather than
     * served in full — the refusal a registered dataset read gets from the DLS/FLS interceptor, which cannot see a
     * dataset the query defines because the name it would key off is not a registered one.
     */
    public void testADocumentLevelSecurityGrantIsRefused() {
        Exception e = expectThrows(Exception.class, () -> query(DLS).close());
        assertThat(
            ExceptionsHelper.unwrapCause(e).getMessage(),
            containsString("document or field level security restrictions are not supported")
        );
    }

    /**
     * The same two refusals for a read that never leaves the coordinator: pinned there, and with no operator above
     * the scan that would need a gather, so the splits are collapsed onto local drivers and no data node request is
     * sent. The coordinator's own gate is the only thing between the caller and the read.
     */
    public void testTheDataSourcePrivilegeIsRequiredOnAReadThatNeverLeavesTheCoordinator() {
        Exception e = expectThrows(Exception.class, () -> collapsedQuery(NO_DATASOURCE).close());
        assertThat(
            ExceptionsHelper.unwrapCause(e).getMessage(),
            containsString("action [" + EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME + "] is unauthorized")
        );
    }

    public void testTheNamePrivilegeIsRequiredOnAReadThatNeverLeavesTheCoordinator() {
        Exception e = expectThrows(Exception.class, () -> collapsedQuery(NO_NAME).close());
        assertThat(
            ExceptionsHelper.unwrapCause(e).getMessage(),
            containsString("action [" + EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME + "] is unauthorized")
        );
    }

    public void testAReadThatNeverLeavesTheCoordinatorStillWorksForTheFullCaller() {
        try (EsqlQueryResponse response = collapsedQuery(FULL)) {
            assertThat(getValuesList(response), hasSize(3));
        }
    }

    /**
     * {@code _name} is the registered dataset's name on the {@code FROM <dataset>} path and NULL here, because a
     * dataset the query defines has no name to report. Pinned so the difference is a decision rather than a
     * discovery.
     */
    public void testTheReadHasNoDatasetNameToReport() {
        String query = "FROM DATASET("
            + DATA_SOURCE
            + ", \""
            + StoragePath.fileUri(fixtureDir)
            + "/*.csv\") METADATA _name | KEEP _name | LIMIT 1";
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        try (
            EsqlQueryResponse response = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(FULL, PASSWORD)))
                .execute(EsqlQueryAction.INSTANCE, request)
                .actionGet(TIMEOUT)
        ) {
            assertThat(getValuesList(response), equalTo(List.of(Collections.singletonList(null))));
        }
    }

    /** Distributed: three files, a SORT above the scan, so the splits are spread over the data nodes. */
    private EsqlQueryResponse query(String user) {
        return query(user, "| SORT a", QueryPragmas.EMPTY);
    }

    /**
     * Collapsed onto the coordinator's own drivers: pinned there, and a plain {@code LIMIT} needs no gather, so
     * {@code ComputeService} reads the splits locally instead of sending itself a data node request.
     */
    private EsqlQueryResponse collapsedQuery(String user) {
        QueryPragmas pinned = new QueryPragmas(
            Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "coordinator_only").build()
        );
        return query(user, "| LIMIT 10", pinned);
    }

    private EsqlQueryResponse query(String user, String suffix, QueryPragmas pragmas) {
        String query = "FROM DATASET(" + DATA_SOURCE + ", \"" + StoragePath.fileUri(fixtureDir) + "/*.csv\") " + suffix;
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        return client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(user, PASSWORD)))
            .execute(EsqlQueryAction.INSTANCE, request)
            .actionGet(TIMEOUT);
    }
}
