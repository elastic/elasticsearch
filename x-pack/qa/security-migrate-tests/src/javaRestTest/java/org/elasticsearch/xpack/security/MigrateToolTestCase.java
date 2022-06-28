/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.notNullValue;

/**
 * {@link MigrateToolTestCase} is an abstract base class to run integration
 * tests against an external Elasticsearch Cluster.
 * <p>
 * You can define a list of transport addresses from where you can reach your cluster
 * by setting "tests.cluster" system property. It defaults to "localhost:9300".
 * <p>
 * All tests can be run from maven using mvn install as maven will start an external cluster first.
 * <p>
 * If you want to debug this module from your IDE, then start an external cluster by yourself
 * then run JUnit. If you changed the default port, set "tests.cluster=localhost:PORT" when running
 * your test.
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public abstract class MigrateToolTestCase extends LuceneTestCase {

    /**
     * Key used to eventually switch to using an external cluster and provide its transport addresses
     */
    public static final String TESTS_CLUSTER = "tests.cluster";

    /**
     * Key used to eventually switch to using an external cluster and provide its transport addresses
     */
    public static final String TESTS_HTTP_CLUSTER = "tests.rest.cluster";

    /**
     * Defaults to localhost:9300
     */
    public static final String TESTS_CLUSTER_DEFAULT = "localhost:9300";

    protected static final Logger logger = LogManager.getLogger(MigrateToolTestCase.class);

    private static final AtomicInteger counter = new AtomicInteger();
    private static Client client;
    private static String clusterAddresses;
    private static String clusterHttpAddresses;

    private static Client startClient(Path tempDir, TransportAddress... transportAddresses) {
        logger.info("--> Starting Elasticsearch Java TransportClient {}, {}", transportAddresses, tempDir);

        Settings clientSettings = Settings.builder()
            .put("cluster.name", "qa_migrate_tests_" + counter.getAndIncrement())
            .put("client.transport.ignore_cluster_name", true)
            .put("path.home", tempDir)
            .put(SecurityField.USER_SETTING.getKey(), "transport_user:x-pack-test-password")
            .build();

        TransportClient client = new PreBuiltXPackTransportClient(clientSettings).addTransportAddresses(transportAddresses);
        Exception clientException = null;
        try {
            logger.info("--> Elasticsearch Java TransportClient started");
            ClusterHealthResponse health = client.admin().cluster().prepareHealth().get();
            logger.info("--> connected to [{}] cluster which is running [{}] node(s).", health.getClusterName(), health.getNumberOfNodes());
        } catch (Exception e) {
            clientException = e;
        }

        assumeNoException("Sounds like your cluster is not running at " + clusterAddresses, clientException);

        return client;
    }

    private static Client startClient() throws UnknownHostException {
        String[] stringAddresses = clusterAddresses.split(",");
        TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            int lastColon = stringAddress.lastIndexOf(":");
            if (lastColon == -1) {
                throw new IllegalArgumentException("address [" + clusterAddresses + "] not valid");
            }
            String ip = stringAddress.substring(0, lastColon);
            String port = stringAddress.substring(lastColon + 1);
            try {
                transportAddresses[i++] = new TransportAddress(InetAddress.getByName(ip), Integer.valueOf(port));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("port is not valid, expected number but was [" + port + "]");
            }
        }
        return startClient(createTempDir(), transportAddresses);
    }

    public static Client getClient() {
        if (client == null) {
            try {
                client = startClient();
            } catch (UnknownHostException e) {
                logger.error("could not start the client", e);
            }
            assertThat(client, notNullValue());
        }
        return client;
    }

    public static String getHttpURL() {
        return clusterHttpAddresses;
    }

    @BeforeClass
    public static void initializeSettings() throws UnknownHostException {
        clusterAddresses = System.getProperty(TESTS_CLUSTER);
        clusterHttpAddresses = System.getProperty(TESTS_HTTP_CLUSTER);
        if (clusterAddresses == null || clusterAddresses.isEmpty()) {
            throw new UnknownHostException("unable to get a cluster address");
        }
    }

    @AfterClass
    public static void stopTransportClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Before
    public void defineIndexName() {
        doClean();
    }

    @After
    public void cleanIndex() {
        doClean();
    }

    private void doClean() {
        if (client != null) {
            try {
                client.admin().indices().prepareDelete("_all").get();
            } catch (Exception e) {
                // We ignore this cleanup exception
            }
        }
    }
}
