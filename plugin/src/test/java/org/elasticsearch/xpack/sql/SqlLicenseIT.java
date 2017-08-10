/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.AbstractLicensesIntegrationTestCase;
import org.elasticsearch.license.License;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableResponse;
import org.elasticsearch.xpack.sql.plugin.cli.action.CliAction;
import org.elasticsearch.xpack.sql.plugin.cli.action.CliResponse;
import org.elasticsearch.xpack.sql.plugin.jdbc.action.JdbcAction;
import org.elasticsearch.xpack.sql.plugin.jdbc.action.JdbcResponse;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Locale;

import static org.elasticsearch.license.XPackLicenseStateTests.randomBasicStandardOrGold;
import static org.elasticsearch.license.XPackLicenseStateTests.randomTrialBasicStandardGoldOrPlatinumMode;
import static org.elasticsearch.license.XPackLicenseStateTests.randomTrialOrPlatinumMode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class SqlLicenseIT extends AbstractLicensesIntegrationTestCase {
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Before
    public void resetLicensing() throws Exception {
        enableJdbcLicensing();
    }

    private static OperationMode randomValidSqlLicenseType() {
        return randomTrialBasicStandardGoldOrPlatinumMode();
    }

    private static OperationMode randomInvalidSqlLicenseType() {
        return OperationMode.MISSING;
    }

    private static OperationMode randomValidJdbcLicenseType() {
        return randomTrialOrPlatinumMode();
    }

    private static OperationMode randomInvalidJdbcLicenseType() {
        return randomBasicStandardOrGold();
    }

    public void enableSqlLicensing() throws Exception {
        updateLicensing(randomValidSqlLicenseType());
    }

    public void disableSqlLicensing() throws Exception {
        updateLicensing(randomInvalidSqlLicenseType());
    }

    public void enableJdbcLicensing() throws Exception {
        updateLicensing(randomValidJdbcLicenseType());
    }

    public void disableJdbcLicensing() throws Exception {
        updateLicensing(randomInvalidJdbcLicenseType());
    }

    public void updateLicensing(OperationMode licenseOperationMode) throws Exception {
        String licenseType = licenseOperationMode.name().toLowerCase(Locale.ROOT);
        wipeAllLicenses();
        if (licenseType.equals("missing")) {
            putLicenseTombstone();
        } else {
            License license = org.elasticsearch.license.TestUtils.generateSignedLicense(licenseType, TimeValue.timeValueMinutes(1));
            putLicense(license);
        }
    }

    public void testSqlActionLicense() throws Exception {
        setupTestIndex();
        disableSqlLicensing();

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareExecute(SqlAction.INSTANCE).query("SELECT * FROM test").get());
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [sql]"));
        enableSqlLicensing();

        SqlResponse response = client().prepareExecute(SqlAction.INSTANCE).query("SELECT * FROM test").get();
        assertThat(response.size(), Matchers.equalTo(2L));
    }

    public void testCliActionLicense() throws Exception {
        setupTestIndex();
        disableSqlLicensing();

        Request request = new CommandRequest("SELECT * FROM test");

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareExecute(CliAction.INSTANCE).request(request).get());
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [sql]"));
        enableSqlLicensing();

        CliResponse response = client().prepareExecute(CliAction.INSTANCE).request(request).get();
        assertThat(response.response(request).toString(), containsString("bar"));
        assertThat(response.response(request).toString(), containsString("baz"));
    }

    public void testJdbcActionLicense() throws Exception {
        setupTestIndex();
        disableJdbcLicensing();

        Request request = new MetaTableRequest("test");

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareExecute(JdbcAction.INSTANCE).request(request).get());
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [jdbc]"));

        enableJdbcLicensing();
        JdbcResponse jdbcResponse = client().prepareExecute(JdbcAction.INSTANCE).request(request).get();
        Response response = jdbcResponse.response(request);
        assertThat(response, instanceOf(MetaTableResponse.class));
    }

    private void setupTestIndex() {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        client().prepareBulk()
                .add(new IndexRequest("test", "doc", "1").source("data", "bar", "count", 42))
                .add(new IndexRequest("test", "doc", "2").source("data", "baz", "count", 43))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
    }
}
