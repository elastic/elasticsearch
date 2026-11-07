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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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

    private static Path fixture;

    @Override
    protected String configUsers() {
        StringBuilder users = new StringBuilder(super.configUsers());
        for (String user : List.of(FULL, NO_DATASOURCE, NO_NAME)) {
            users.append(user).append(':').append(SecuritySettingsSource.TEST_PASSWORD_HASHED).append('\n');
        }
        return users.toString();
    }

    @Override
    protected String configUsersRoles() {
        StringBuilder usersRoles = new StringBuilder(super.configUsersRoles());
        for (String user : List.of(FULL, NO_DATASOURCE, NO_NAME)) {
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
        if (fixture == null) {
            Path dir = createTempDir();
            fixture = Files.writeString(dir.resolve("a.csv"), "a\na1\na2\n", StandardCharsets.UTF_8);
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
            assertThat(getValuesList(response), equalTo(List.of(List.of("a1"), List.of("a2"))));
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
     * The same two refusals for a read pinned to the coordinator, where no data node request is sent at all: without
     * the coordinator's own gate nothing would be asked, and the query would read under a privilege the caller does
     * not hold.
     */
    public void testTheDataSourcePrivilegeIsRequiredOnACoordinatorOnlyRead() {
        Exception e = expectThrows(Exception.class, () -> query(NO_DATASOURCE, coordinatorOnly()).close());
        assertThat(
            ExceptionsHelper.unwrapCause(e).getMessage(),
            containsString("action [" + EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME + "] is unauthorized")
        );
    }

    public void testTheNamePrivilegeIsRequiredOnACoordinatorOnlyRead() {
        Exception e = expectThrows(Exception.class, () -> query(NO_NAME, coordinatorOnly()).close());
        assertThat(
            ExceptionsHelper.unwrapCause(e).getMessage(),
            containsString("action [" + EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME + "] is unauthorized")
        );
    }

    public void testACoordinatorOnlyReadStillWorksForTheFullCaller() {
        try (EsqlQueryResponse response = query(FULL, coordinatorOnly())) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("a1"), List.of("a2"))));
        }
    }

    /** Keeps every split on the coordinator, so no data node request carries the definition anywhere. */
    private static QueryPragmas coordinatorOnly() {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "coordinator_only").build());
    }

    private EsqlQueryResponse query(String user) {
        return query(user, QueryPragmas.EMPTY);
    }

    private EsqlQueryResponse query(String user, QueryPragmas pragmas) {
        String query = "FROM DATASET(" + DATA_SOURCE + ", \"" + StoragePath.fileUri(fixture) + "\") | SORT a";
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        return client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(user, PASSWORD)))
            .execute(EsqlQueryAction.INSTANCE, request)
            .actionGet(TIMEOUT);
    }
}
