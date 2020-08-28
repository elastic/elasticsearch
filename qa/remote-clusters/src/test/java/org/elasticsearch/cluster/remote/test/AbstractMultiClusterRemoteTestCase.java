/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.remote.test;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

public abstract class AbstractMultiClusterRemoteTestCase extends ESRestTestCase {

    private static final String USER = "x_pack_rest_user";
    private static final String PASS = "x-pack-test-password";
    private static final String KEYSTORE_PASS = "testnode";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    private static RestHighLevelClient cluster1Client;
    private static RestHighLevelClient cluster2Client;
    private static boolean initialized = false;


    @Override
    protected String getTestRestCluster() {
        return "localhost:" + getProperty("test.fixtures.elasticsearch-" + getDistribution() + "-1.tcp.9200");
    }

    @Before
    public void initClientsAndConfigureClusters() throws Exception {
        if (initialized) {
            return;
        }

        cluster1Client = buildClient("localhost:" + getProperty("test.fixtures.elasticsearch-" + getDistribution() + "-1.tcp.9200"));
        cluster2Client = buildClient("localhost:" + getProperty("test.fixtures.elasticsearch-" + getDistribution() + "-2.tcp.9200"));

        cluster1Client().cluster().health(new ClusterHealthRequest().waitForNodes("1").waitForYellowStatus(), RequestOptions.DEFAULT);
        cluster2Client().cluster().health(new ClusterHealthRequest().waitForNodes("1").waitForYellowStatus(), RequestOptions.DEFAULT);

        initialized = true;
    }

    protected String getDistribution() {
        String distribution = System.getProperty("tests.distribution", "default");
        if (distribution.equals("oss") == false && distribution.equals("default") == false) {
            throw new IllegalArgumentException("supported values for tests.distribution are oss or default but it was " + distribution);
        }
        return distribution;
    }

    @AfterClass
    public static void destroyClients() throws IOException {
        try {
            IOUtils.close(cluster1Client, cluster2Client);
        } finally {
            cluster1Client = null;
            cluster2Client = null;
        }
    }

    protected static RestHighLevelClient cluster1Client() {
        return cluster1Client;
    }

    protected static RestHighLevelClient cluster2Client() {
        return cluster2Client;
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, RestClient::close, Collections.emptyList());
        }
    }

    private RestHighLevelClient buildClient(final String url) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)), getProtocol());
        return new HighLevelClient(buildClient(restAdminSettings(), new HttpHost[]{httpHost}));
    }

    protected boolean isOss() {
        return getDistribution().equals("oss");
    }

    static Path keyStore;

    @BeforeClass
    public static void getKeyStore() {
        try {
            keyStore = PathUtils.get(AbstractMultiClusterRemoteTestCase.class.getResource("/testnode.jks").toURI());
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("exception while reading the store", e);
        }
        if (Files.exists(keyStore) == false) {
            throw new IllegalStateException("Keystore file [" + keyStore + "] does not exist.");
        }
    }

    @AfterClass
    public static void clearKeyStore() {
        keyStore = null;
    }

    @Override
    protected Settings restClientSettings() {
        if (isOss()) {
            return super.restClientSettings();
        }
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(ESRestTestCase.TRUSTSTORE_PATH, keyStore)
            .put(ESRestTestCase.TRUSTSTORE_PASSWORD, KEYSTORE_PASS)
            .build();
    }

    @Override
    protected String getProtocol() {
        if (isOss()) {
            return "http";
        }
        return "https";
    }

    private static String basicAuthHeaderValue(String username, SecureString passwd) {
        CharBuffer chars = CharBuffer.allocate(username.length() + passwd.length() + 1);
        byte[] charBytes = null;
        try {
            chars.put(username).put(':').put(passwd.getChars());
            charBytes = CharArrays.toUtf8Bytes(chars.array());

            //TODO we still have passwords in Strings in headers. Maybe we can look into using a CharSequence?
            String basicToken = Base64.getEncoder().encodeToString(charBytes);
            return "Basic " + basicToken;
        } finally {
            Arrays.fill(chars.array(), (char) 0);
            if (charBytes != null) {
                Arrays.fill(charBytes, (byte) 0);
            }
        }
    }

    private String getProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            throw new IllegalStateException("Could not find system properties from test.fixtures. " +
                "This test expects to run with the elasticsearch.test.fixtures Gradle plugin");
        }
        return value;
    }
}
