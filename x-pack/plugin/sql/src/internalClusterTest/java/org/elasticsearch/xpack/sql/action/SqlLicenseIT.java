/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.AbstractLicensesIntegrationTestCase;
import org.elasticsearch.license.License;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Collection;
import java.util.Locale;

import static org.elasticsearch.license.XPackLicenseStateTests.randomBasicStandardOrGold;
import static org.elasticsearch.license.XPackLicenseStateTests.randomTrialBasicStandardGoldOrPlatinumMode;
import static org.elasticsearch.license.XPackLicenseStateTests.randomTrialOrPlatinumMode;
import static org.hamcrest.Matchers.equalTo;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/37320")
public class SqlLicenseIT extends AbstractLicensesIntegrationTestCase {

    @Before
    public void resetLicensing() throws Exception {
        enableJdbcLicensing();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Add Netty so we can test JDBC licensing because only exists on the REST layer.
        return CollectionUtils.appendToCopy(super.nodePlugins(), Netty4Plugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // Enable http so we can test JDBC licensing because only exists on the REST layer.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .build();
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

    public void testSqlQueryActionLicense() throws Exception {
        setupTestIndex();
        disableSqlLicensing();

        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            new SqlQueryRequestBuilder(client()).query("SELECT * FROM test")
        );
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [sql]"));
        enableSqlLicensing();

        SqlQueryResponse response = new SqlQueryRequestBuilder(client()).query("SELECT * FROM test").get();
        assertThat(response.size(), Matchers.equalTo(2L));
    }

    public void testSqlQueryActionJdbcModeLicense() throws Exception {
        setupTestIndex();
        disableJdbcLicensing();

        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            new SqlQueryRequestBuilder(client()).query("SELECT * FROM test").mode("jdbc")
        );
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [jdbc]"));
        enableJdbcLicensing();

        SqlQueryResponse response = new SqlQueryRequestBuilder(client()).query("SELECT * FROM test").mode("jdbc").get();
        assertThat(response.size(), Matchers.equalTo(2L));
    }

    public void testSqlTranslateActionLicense() throws Exception {
        setupTestIndex();
        disableSqlLicensing();

        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            new SqlTranslateRequestBuilder(client()).query("SELECT * FROM test")
        );
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [sql]"));
        enableSqlLicensing();

        SqlTranslateResponse response = new SqlTranslateRequestBuilder(client()).query("SELECT * FROM test").get();
        SearchSourceBuilder source = response.source();
        assertThat(source.docValueFields(), Matchers.contains(new FieldAndFormat("count", null)));
        FetchSourceContext fetchSource = source.fetchSource();
        assertThat(fetchSource.includes(), Matchers.arrayContaining("data"));
    }

    // TODO test SqlGetIndicesAction. Skipping for now because of lack of serialization support.

    private void setupTestIndex() {
        ElasticsearchAssertions.assertAcked(indicesAdmin().prepareCreate("test").get());
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("data", "bar", "count", 42))
            .add(new IndexRequest("test").id("2").source("data", "baz", "count", 43))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

}
