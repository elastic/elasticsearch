/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.AbstractLicensesIntegrationTestCase;
import org.elasticsearch.license.License;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;
import org.elasticsearch.xpack.sql.plugin.SqlTranslateAction;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.license.XPackLicenseStateTests.randomBasicStandardOrGold;
import static org.elasticsearch.license.XPackLicenseStateTests.randomTrialBasicStandardGoldOrPlatinumMode;
import static org.elasticsearch.license.XPackLicenseStateTests.randomTrialOrPlatinumMode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SqlLicenseIT extends AbstractLicensesIntegrationTestCase {
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Before
    public void resetLicensing() throws Exception {
        enableJdbcLicensing();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Add Netty so we can test JDBC licensing because only exists on the REST layer.
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(Netty4Plugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // Enable http so we can test JDBC licensing because only exists on the REST layer.
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
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

    public void testSqlTranslateActionLicense() throws Exception {
        setupTestIndex();
        disableSqlLicensing();

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareExecute(SqlTranslateAction.INSTANCE).query("SELECT * FROM test").get());
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [sql]"));
        enableSqlLicensing();

        SqlTranslateAction.Response response = client().prepareExecute(SqlTranslateAction.INSTANCE).query("SELECT * FROM test").get();
        SearchSourceBuilder source = response.source();
        assertThat(source.docValueFields(), Matchers.contains("count"));
        FetchSourceContext fetchSource = source.fetchSource();
        assertThat(fetchSource.includes(), Matchers.arrayContaining("data"));
    }

    public void testJdbcActionLicense() throws Exception {
        setupTestIndex();
        disableJdbcLicensing();

        Request request = new MetaTableRequest("test");
        ResponseException responseException = expectThrows(ResponseException.class, () -> jdbc(request));
        assertThat(responseException.getMessage(), containsString("current license is non-compliant for [jdbc]"));
        assertThat(responseException.getMessage(), containsString("security_exception"));


        enableJdbcLicensing();
        Response response = jdbc(request);
        assertThat(response, instanceOf(MetaTableResponse.class));
    }

    private Response jdbc(Request request) throws IOException {
        // Convert the request to the HTTP entity that JDBC uses
        HttpEntity entity;
        try (BytesStreamOutput bytes = new BytesStreamOutput()) {
            DataOutput out = new DataOutputStream(bytes);
            Proto.INSTANCE.writeRequest(request, out);
            entity = new ByteArrayEntity(BytesRef.deepCopyOf(bytes.bytes().toBytesRef()).bytes, ContentType.APPLICATION_JSON);
        }

        // Execute
        InputStream response = getRestClient().performRequest("POST", "/_sql/jdbc", emptyMap(), entity).getEntity().getContent();

        // Deserialize bytes to response like JDBC does
        try {
            DataInput in = new DataInputStream(response);
            return Proto.INSTANCE.readResponse(request, in);
        } finally {
            response.close();
        }
    }

    // TODO test SqlGetIndicesAction. Skipping for now because of lack of serialization support.

    private void setupTestIndex() {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        client().prepareBulk()
                .add(new IndexRequest("test", "doc", "1").source("data", "bar", "count", 42))
                .add(new IndexRequest("test", "doc", "2").source("data", "baz", "count", 43))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
    }

}
