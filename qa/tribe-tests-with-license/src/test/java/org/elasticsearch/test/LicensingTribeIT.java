/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.GetLicenseResponse;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensesStatus;
import org.elasticsearch.license.LicensingClient;
import org.elasticsearch.license.PutLicenseResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.junit.AfterClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;

public class LicensingTribeIT extends ESIntegTestCase {
    private static TestCluster cluster2;
    private static TestCluster tribeNode;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(XPackPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (cluster2 == null) {
            cluster2 = buildExternalCluster(System.getProperty("tests.cluster2"));
        }
        if (tribeNode == null) {
            tribeNode = buildExternalCluster(System.getProperty("tests.tribe"));
        }
    }


    @AfterClass
    public static void tearDownExternalClusters() throws IOException {
        if (cluster2 != null) {
            try {
                cluster2.close();
            } finally {
                cluster2 = null;
            }
        }
        if (tribeNode != null) {
            try {
                tribeNode.close();
            } finally {
                tribeNode = null;
            }
        }
    }


    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        builder.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        builder.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        builder.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        return builder.build();
    }

    private ExternalTestCluster buildExternalCluster(String clusterAddresses) throws IOException {
        String[] stringAddresses = clusterAddresses.split(",");
        TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            URL url = new URL("http://" + stringAddress);
            InetAddress inetAddress = InetAddress.getByName(url.getHost());
            transportAddresses[i++] = new TransportAddress(new InetSocketAddress(inetAddress, url.getPort()));
        }
        return new ExternalTestCluster(createTempDir(), externalClusterClientSettings(), transportClientPlugins(), transportAddresses);
    }

    public void testLicensePropagateToTribeNode() throws Exception {
        // test that auto-generated trial license propagates to tribe
        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = new LicensingClient(tribeNode.client()).prepareGetLicense().get();
            assertNotNull(getLicenseResponse.license());
            assertThat(getLicenseResponse.license().operationMode(), equalTo(License.OperationMode.TRIAL));
        });

        // test that signed license put in one cluster propagates to tribe
        LicensingClient cluster1Client = new LicensingClient(client());
        PutLicenseResponse licenseResponse = cluster1Client
                .preparePutLicense(License.fromSource(new BytesArray(BASIC_LICENSE.getBytes(StandardCharsets.UTF_8)), XContentType.JSON))
                .setAcknowledge(true).get();
        assertThat(licenseResponse.isAcknowledged(), equalTo(true));
        assertThat(licenseResponse.status(), equalTo(LicensesStatus.VALID));
        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = new LicensingClient(tribeNode.client()).prepareGetLicense().get();
            assertNotNull(getLicenseResponse.license());
            assertThat(getLicenseResponse.license().operationMode(), equalTo(License.OperationMode.BASIC));
        });

        // test that signed license with higher operation mode takes precedence
        LicensingClient cluster2Client = new LicensingClient(cluster2.client());
        licenseResponse = cluster2Client
                .preparePutLicense(License.fromSource(new BytesArray(PLATINUM_LICENSE.getBytes(StandardCharsets.UTF_8)), XContentType.JSON))
                .setAcknowledge(true).get();
        assertThat(licenseResponse.isAcknowledged(), equalTo(true));
        assertThat(licenseResponse.status(), equalTo(LicensesStatus.VALID));
        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = new LicensingClient(tribeNode.client()).prepareGetLicense().get();
            assertNotNull(getLicenseResponse.license());
            assertThat(getLicenseResponse.license().operationMode(), equalTo(License.OperationMode.PLATINUM));
        });

        // test removing signed license falls back works
        assertTrue(cluster2Client.prepareDeleteLicense().get().isAcknowledged());
        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = new LicensingClient(tribeNode.client()).prepareGetLicense().get();
            assertNotNull(getLicenseResponse.license());
            assertThat(getLicenseResponse.license().operationMode(), equalTo(License.OperationMode.BASIC));
        });
    }

    private static final String PLATINUM_LICENSE = "{\"license\":{\"uid\":\"1\",\"type\":\"platinum\"," +
            "\"issue_date_in_millis\":1411948800000,\"expiry_date_in_millis\":1914278399999,\"max_nodes\":1," +
            "\"issued_to\":\"issuedTo\",\"issuer\":\"issuer\"," +
            "\"signature\":\"AAAAAwAAAA2hWlkvKcxQIpdVWdCtAAABmC9ZN0hjZDBGYnVyRXpCOW5Bb3FjZDAxOWpSbTVoMVZwUzRxVk1" +
            "PSmkxakxZdW5IMlhlTHNoN1N2MXMvRFk4d3JTZEx3R3RRZ0pzU3lobWJKZnQvSEFva0ppTHBkWkprZWZSQi9iNmRQNkw1SlpLN0l" +
            "DalZCS095MXRGN1lIZlpYcVVTTnFrcTE2dzhJZmZrdFQrN3JQeGwxb0U0MXZ0dDJHSERiZTVLOHNzSDByWnpoZEphZHBEZjUrTVB" +
            "xRENNSXNsWWJjZllaODdzVmEzUjNiWktNWGM5TUhQV2plaUo4Q1JOUml4MXNuL0pSOEhQaVB2azhmUk9QVzhFeTFoM1Q0RnJXSG5" +
            "3MWk2K055c28zSmRnVkF1b2JSQkFLV2VXUmVHNDZ2R3o2VE1qbVNQS2lxOHN5bUErZlNIWkZSVmZIWEtaSU9wTTJENDVvT1NCYkla" +
            "cUYyK2FwRW9xa0t6dldMbmMzSGtQc3FWOTgzZ3ZUcXMvQkt2RUZwMFJnZzlvL2d2bDRWUzh6UG5pdENGWFRreXNKNkE9PQAAAQBWg" +
            "u3yZp0KOBG//92X4YVmau3P5asvx0FAPDX2Ze734Tap/nc30X6Rt4yEEm+6bCQr/ibBOqWboJKRbbTZLBQfYFmL1ZqvAY3bJJ1/Xs" +
            "8NyDfxKGztlUt/IIOzHPzxs0f8Bv4OJeK48vjovWaDc1Vmo4n1SGyyL0JcEbOWC6A3U3mBsWn7wLUe+hW9+akVAYOO5TIcm60ub7k" +
            "H/LIZNOhvGglSVDbl3p8EBkNMy0CV7urQ0wdG1nLCnvf8/BiT15lC5nLrM9Dt5w3pzciPlASzw4iksW/CzvYy5tjOoWKEnxi2EZOB" +
            "9dKyT4mTdvyBOrTHLdgr4lmHd3qYAEgcTCaQ\",\"start_date_in_millis\":-1}}";

    private static final String BASIC_LICENSE = "{\"license\":{\"uid\":\"1\",\"type\":\"basic\"," +
            "\"issue_date_in_millis\":1411948800000,\"expiry_date_in_millis\":1914278399999,\"max_nodes\":1," +
            "\"issued_to\":\"issuedTo\",\"issuer\":\"issuer\",\"signature\":\"AAA" + "AAwAAAA2is2oANL3mZGS883l9AAAB" +
            "mC9ZN0hjZDBGYnVyRXpCOW5Bb3FjZDAxOWpSbTVoMVZwUzRxVk1PSmkxakxZdW5IMlhlTHNoN1N2MXMvRFk4d3JTZEx3R3RRZ0pzU3" +
            "lobWJKZnQvSEFva0ppTHBkWkprZWZSQi9iNmRQNkw1SlpLN0lDalZCS095MXRGN1lIZlpYcVVTTnFrcTE2dzhJZmZrdFQrN3JQeGwx" +
            "b0U0MXZ0dDJHSERiZTVLOHNzSDByWnpoZEphZHBEZjUrTVBxRENNSXNsWWJjZllaODdzVmEzUjNiWktNWGM5TUhQV2plaUo4Q1JOUm" +
            "l4MXNuL0pSOEhQaVB2azhmUk9QVzhFeTFoM1Q0RnJXSG53MWk2K055c28zSmRnVkF1b2JSQkFLV2VXUmVHNDZ2R3o2VE1qbVNQS2lx" +
            "OHN5bUErZlNIWkZSVmZIWEtaSU9wTTJENDVvT1NCYklacUYyK2FwRW9xa0t6dldMbmMzSGtQc3FWOTgzZ3ZUcXMvQkt2RUZwMFJnZz" +
            "lvL2d2bDRWUzh6UG5pdENGWFRreXNKNkE9PQAAAQCjL9HJnHrHVRq39yO5OFrOS0fY+mf+KqLh8i+RK4s9Hepdi/VQ3SHTEonEUCCB" +
            "1iFO35eykW3t+poCMji9VGkslQyJ+uWKzUqn0lmioy8ukpjETcmKH8TSWTqcC7HNZ0NKc1XMTxwkIi/chQTsPUz+h3gfCHZRQwGnRz" +
            "JPmPjCJf4293hsMFUlsFQU3tYKDH+kULMdNx1Cg+3PhbUCNrUyQJMb5p4XDrwOaanZUM6HdifS1Y/qjxLXC/B1wHGFEpvrEPFyBuSe" +
            "GnJ9uxkrBSv28iG0qsyHrFhHQXIMVFlQKCPaMKikfuZyRhxzE5ntTcGJMn84llCaIyX/kmzqoZHQ\",\"start_date_in_millis\":-1}}\n";
}
