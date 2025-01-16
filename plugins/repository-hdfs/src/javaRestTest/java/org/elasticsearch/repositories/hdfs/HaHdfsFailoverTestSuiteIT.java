/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.BadFencingConfigurationException;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.hadoop.ha.ZKFCProtocol;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration test that runs against an HA-Enabled HDFS instance
 */
public class HaHdfsFailoverTestSuiteIT extends ESRestTestCase {

    public void testHAFailoverWithRepository() throws Exception {
        RestClient client = client();

        String esKerberosPrincipal = System.getProperty("test.krb5.principal.es");
        String hdfsKerberosPrincipal = System.getProperty("test.krb5.principal.hdfs");
        String kerberosKeytabLocation = System.getProperty("test.krb5.keytab.hdfs");
        String ports = System.getProperty("test.hdfs-fixture.ports");
        String nn1Port = "10001";
        String nn2Port = "10002";
        if (ports.length() > 0) {
            final Path path = PathUtils.get(ports);
            final List<String> lines = AccessController.doPrivileged((PrivilegedExceptionAction<List<String>>) () -> {
                return Files.readAllLines(path);
            });
            nn1Port = lines.get(0);
            nn2Port = lines.get(1);
        }
        boolean securityEnabled = hdfsKerberosPrincipal != null;

        Configuration hdfsConfiguration = new Configuration();
        hdfsConfiguration.set("dfs.nameservices", "ha-hdfs");
        hdfsConfiguration.set("dfs.ha.namenodes.ha-hdfs", "nn1,nn2");
        hdfsConfiguration.set("dfs.namenode.rpc-address.ha-hdfs.nn1", "localhost:" + nn1Port);
        hdfsConfiguration.set("dfs.namenode.rpc-address.ha-hdfs.nn2", "localhost:" + nn2Port);
        hdfsConfiguration.set(
            "dfs.client.failover.proxy.provider.ha-hdfs",
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );

        AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
            if (securityEnabled) {
                // ensure that keytab exists
                Path kt = PathUtils.get(kerberosKeytabLocation);
                if (Files.exists(kt) == false) {
                    throw new IllegalStateException("Could not locate keytab at " + kerberosKeytabLocation);
                }
                if (Files.isReadable(kt) != true) {
                    throw new IllegalStateException("Could not read keytab at " + kerberosKeytabLocation);
                }
                logger.info("Keytab Length: " + Files.readAllBytes(kt).length);

                // set principal names
                hdfsConfiguration.set("dfs.namenode.kerberos.principal", hdfsKerberosPrincipal);
                hdfsConfiguration.set("dfs.datanode.kerberos.principal", hdfsKerberosPrincipal);
                hdfsConfiguration.set("dfs.data.transfer.protection", "authentication");

                SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, hdfsConfiguration);
                UserGroupInformation.setConfiguration(hdfsConfiguration);
                UserGroupInformation.loginUserFromKeytab(hdfsKerberosPrincipal, kerberosKeytabLocation);
            } else {
                SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.SIMPLE, hdfsConfiguration);
                UserGroupInformation.setConfiguration(hdfsConfiguration);
                UserGroupInformation.getCurrentUser();
            }
            return null;
        });

        // Create repository
        {
            Request request = new Request("PUT", "/_snapshot/hdfs_ha_repo_read");
            request.setJsonEntity(
                "{"
                    + "\"type\":\"hdfs\","
                    + "\"settings\":{"
                    + "\"uri\": \"hdfs://ha-hdfs/\",\n"
                    + "\"path\": \"/user/elasticsearch/existing/readonly-repository\","
                    + "\"readonly\": \"true\","
                    + securityCredentials(securityEnabled, esKerberosPrincipal)
                    + "\"conf.dfs.nameservices\": \"ha-hdfs\","
                    + "\"conf.dfs.ha.namenodes.ha-hdfs\": \"nn1,nn2\","
                    + "\"conf.dfs.namenode.rpc-address.ha-hdfs.nn1\": \"localhost:"
                    + nn1Port
                    + "\","
                    + "\"conf.dfs.namenode.rpc-address.ha-hdfs.nn2\": \"localhost:"
                    + nn2Port
                    + "\","
                    + "\"conf.dfs.client.failover.proxy.provider.ha-hdfs\": "
                    + "\"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\""
                    + "}"
                    + "}"
            );
            Response response = client.performRequest(request);

            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        }

        // Get repository
        {
            Response response = client.performRequest(new Request("GET", "/_snapshot/hdfs_ha_repo_read/_all"));
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        }

        // Failover the namenode to the second.
        failoverHDFS("nn1", "nn2", hdfsConfiguration);

        // Get repository again
        {
            Response response = client.performRequest(new Request("GET", "/_snapshot/hdfs_ha_repo_read/_all"));
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        }
    }

    private String securityCredentials(boolean securityEnabled, String kerberosPrincipal) {
        if (securityEnabled) {
            return "\"security.principal\": \"" + kerberosPrincipal + "\"," + "\"conf.dfs.data.transfer.protection\": \"authentication\",";
        } else {
            return "";
        }
    }

    /**
     * Wraps an HAServiceTarget, keeping track of any HAServiceProtocol proxies it generates in order
     * to close them at the end of the test lifecycle.
     */
    private static class CloseableHAServiceTarget extends HAServiceTarget {
        private final HAServiceTarget delegate;
        private final List<HAServiceProtocol> protocolsToClose = new ArrayList<>();

        CloseableHAServiceTarget(HAServiceTarget delegate) {
            this.delegate = delegate;
        }

        @Override
        public InetSocketAddress getAddress() {
            return delegate.getAddress();
        }

        @Override
        public InetSocketAddress getHealthMonitorAddress() {
            return delegate.getHealthMonitorAddress();
        }

        @Override
        public InetSocketAddress getZKFCAddress() {
            return delegate.getZKFCAddress();
        }

        @Override
        public NodeFencer getFencer() {
            return delegate.getFencer();
        }

        @Override
        public void checkFencingConfigured() throws BadFencingConfigurationException {
            delegate.checkFencingConfigured();
        }

        @Override
        public HAServiceProtocol getProxy(Configuration conf, int timeoutMs) throws IOException {
            HAServiceProtocol proxy = delegate.getProxy(conf, timeoutMs);
            protocolsToClose.add(proxy);
            return proxy;
        }

        @Override
        public HAServiceProtocol getHealthMonitorProxy(Configuration conf, int timeoutMs) throws IOException {
            return delegate.getHealthMonitorProxy(conf, timeoutMs);
        }

        @Override
        public ZKFCProtocol getZKFCProxy(Configuration conf, int timeoutMs) throws IOException {
            return delegate.getZKFCProxy(conf, timeoutMs);
        }

        @Override
        public boolean isAutoFailoverEnabled() {
            return delegate.isAutoFailoverEnabled();
        }

        private void close() {
            for (HAServiceProtocol protocol : protocolsToClose) {
                if (protocol instanceof HAServiceProtocolClientSideTranslatorPB) {
                    ((HAServiceProtocolClientSideTranslatorPB) protocol).close();
                }
            }
        }
    }

    /**
     * The default HAAdmin tool does not throw exceptions on failures, and does not close any client connection
     * resources when it concludes. This subclass overrides the tool to allow for exception throwing, and to
     * keep track of and clean up connection resources.
     */
    private static class CloseableHAAdmin extends DFSHAAdmin {
        private final List<CloseableHAServiceTarget> serviceTargets = new ArrayList<>();

        @Override
        protected HAServiceTarget resolveTarget(String nnId) {
            CloseableHAServiceTarget target = new CloseableHAServiceTarget(super.resolveTarget(nnId));
            serviceTargets.add(target);
            return target;
        }

        @Override
        public int run(String[] argv) throws Exception {
            return runCmd(argv);
        }

        public int transitionToStandby(String namenodeID) throws Exception {
            return run(new String[] { "-transitionToStandby", namenodeID });
        }

        public int transitionToActive(String namenodeID) throws Exception {
            return run(new String[] { "-transitionToActive", namenodeID });
        }

        public void close() {
            for (CloseableHAServiceTarget serviceTarget : serviceTargets) {
                serviceTarget.close();
            }
        }
    }

    /**
     * Performs a two-phase leading namenode transition.
     * @param from Namenode ID to transition to standby
     * @param to Namenode ID to transition to active
     * @param configuration Client configuration for HAAdmin tool
     * @throws IOException In the event of a raised exception during namenode failover.
     */
    private void failoverHDFS(String from, String to, Configuration configuration) throws IOException {
        logger.info("Swapping active namenodes: [{}] to standby and [{}] to active", from, to);
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                CloseableHAAdmin haAdmin = new CloseableHAAdmin();
                haAdmin.setConf(configuration);
                try {
                    haAdmin.transitionToStandby(from);
                    haAdmin.transitionToActive(to);
                } finally {
                    haAdmin.close();
                }
                return null;
            });
        } catch (PrivilegedActionException pae) {
            throw new IOException("Unable to perform namenode failover", pae);
        }
    }
}
