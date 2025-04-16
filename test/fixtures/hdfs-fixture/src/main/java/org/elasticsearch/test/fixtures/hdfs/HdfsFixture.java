/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.ha.BadFencingConfigurationException;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.hadoop.ha.ZKFCProtocol;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class HdfsFixture extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFixture.class);

    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private MiniDFSCluster dfs;
    private String haNameService;
    private Supplier<String> principalConfig = null;
    private Supplier<Path> keytab = null;
    private Configuration cfg;

    private Configuration haConfiguration;
    private int explicitPort = findAvailablePort();

    public HdfsFixture withHAService(String haNameService) {
        this.haNameService = haNameService;
        return this;
    }

    public HdfsFixture withKerberos(Supplier<String> principalConfig, Supplier<Path> keytabFile) {
        this.principalConfig = principalConfig;
        this.keytab = keytabFile;
        return this;
    }

    @Override
    protected void before() throws Throwable {
        temporaryFolder.create();
        assumeHdfsAvailable();
        startMinHdfs();
    }

    private void assumeHdfsAvailable() {
        boolean fixtureSupported = false;
        if (isWindows()) {
            // hdfs fixture will not start without hadoop native libraries on windows
            String nativePath = System.getenv("HADOOP_HOME");
            if (nativePath != null) {
                java.nio.file.Path path = Paths.get(nativePath);
                if (Files.isDirectory(path)
                    && Files.exists(path.resolve("bin").resolve("winutils.exe"))
                    && Files.exists(path.resolve("bin").resolve("hadoop.dll"))
                    && Files.exists(path.resolve("bin").resolve("hdfs.dll"))) {
                    fixtureSupported = true;
                } else {
                    throw new IllegalStateException(
                        "HADOOP_HOME: " + path + " is invalid, does not contain hadoop native libraries in " + nativePath + "/bin"
                    );
                }
            }
        } else {
            fixtureSupported = true;
        }

        boolean nonLegalegalPath = temporaryFolder.getRoot().getAbsolutePath().contains(" ");
        if (nonLegalegalPath) {
            fixtureSupported = false;
        }

        Assume.assumeTrue("HDFS Fixture is not supported", fixtureSupported);
    }

    private boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }

    /**
     * Performs a two-phase leading namenode transition.
     * @param from Namenode ID to transition to standby
     * @param to Namenode ID to transition to active
     * @throws IOException In the event of a raised exception during namenode failover.
     */
    public void failoverHDFS(String from, String to) throws IOException {
        assert isHA() && haConfiguration != null : "HA Configuration must be set up before performing failover";
        LOGGER.info("Swapping active namenodes: [{}] to standby and [{}] to active", from, to);
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                CloseableHAAdmin haAdmin = new CloseableHAAdmin();
                haAdmin.setConf(haConfiguration);
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

    public void setupHA() throws IOException {
        assert isHA() : "HA Name Service must be set up before setting up HA";
        haConfiguration = new Configuration();
        haConfiguration.set("dfs.nameservices", haNameService);
        haConfiguration.set("dfs.ha.namenodes.ha-hdfs", "nn1,nn2");
        haConfiguration.set("dfs.namenode.rpc-address.ha-hdfs.nn1", "localhost:" + getPort(0));
        haConfiguration.set("dfs.namenode.rpc-address.ha-hdfs.nn2", "localhost:" + (getPort(1)));
        haConfiguration.set(
            "dfs.client.failover.proxy.provider.ha-hdfs",
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );

        if (isSecure()) {
            // ensure that keytab exists
            Path kt = this.keytab.get();
            if (Files.exists(kt) == false) {
                throw new IllegalStateException("Could not locate keytab at " + keytab.get());
            }
            if (Files.isReadable(kt) != true) {
                throw new IllegalStateException("Could not read keytab at " + keytab.get());
            }
            LOGGER.info("Keytab Length: " + Files.readAllBytes(kt).length);

            // set principal names
            String hdfsKerberosPrincipal = principalConfig.get();
            haConfiguration.set("dfs.namenode.kerberos.principal", hdfsKerberosPrincipal);
            haConfiguration.set("dfs.datanode.kerberos.principal", hdfsKerberosPrincipal);
            haConfiguration.set("dfs.data.transfer.protection", "authentication");

            SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, haConfiguration);
            UserGroupInformation.setConfiguration(haConfiguration);
            UserGroupInformation.loginUserFromKeytab(hdfsKerberosPrincipal, keytab.get().toString());
        } else {
            SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.SIMPLE, haConfiguration);
            UserGroupInformation.setConfiguration(haConfiguration);
            UserGroupInformation.getCurrentUser();
        }
    }

    private void startMinHdfs() throws Exception {
        Path baseDir = temporaryFolder.newFolder("baseDir").toPath();
        int maxAttempts = 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                Path hdfsHome = createHdfsDataFolder(baseDir);
                tryStartingHdfs(hdfsHome);
                break;
            } catch (IOException e) {
                // Log the exception
                System.out.println("Attempt " + attempt + " failed with error: " + e.getMessage());
                // If the maximum number of attempts is reached, rethrow the exception
                FileUtils.deleteDirectory(baseDir.toFile());
                if (attempt == maxAttempts) {
                    Assume.assumeTrue("Unable to start HDFS cluster", false);
                }
            }
        }
    }

    private static Path createHdfsDataFolder(Path baseDir) throws IOException {
        if (System.getenv("HADOOP_HOME") == null) {
            Path hadoopHome = baseDir.resolve("hadoop-home");
            Files.createDirectories(hadoopHome);
            System.setProperty("hadoop.home.dir", hadoopHome.toAbsolutePath().toString());
        }
        // hdfs-data/, where any data is going
        Path hdfsData = baseDir.resolve("hdfs-data");
        Path data = hdfsData.resolve("data");
        Files.createDirectories(data);
        return hdfsData;
    }

    private void tryStartingHdfs(Path hdfsHome) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
        InvocationTargetException, IOException, URISyntaxException {
        // configure cluster
        cfg = new Configuration();
        cfg.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsHome.toAbsolutePath().toString());
        cfg.set("hadoop.security.group.mapping", "org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback");

        // optionally configure security
        if (isSecure()) {
            String kerberosPrincipal = principalConfig.get();
            String keytabFilePath = keytab.get().toString();
            cfg.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            cfg.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
            cfg.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, kerberosPrincipal);
            cfg.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, kerberosPrincipal);
            cfg.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, kerberosPrincipal);
            cfg.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytabFilePath);
            cfg.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytabFilePath);
            cfg.set(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, "true");
            cfg.set(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, "true");
            cfg.set(DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY, "true");
            cfg.set(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, "true");
        }
        refreshKrb5Config();
        UserGroupInformation.setConfiguration(cfg);

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(cfg);
        builder.nameNodePort(explicitPort);
        if (isHA()) {
            MiniDFSNNTopology.NNConf nn1 = new MiniDFSNNTopology.NNConf("nn1").setIpcPort(0);
            MiniDFSNNTopology.NNConf nn2 = new MiniDFSNNTopology.NNConf("nn2").setIpcPort(0);
            MiniDFSNNTopology.NSConf nameservice = new MiniDFSNNTopology.NSConf(haNameService).addNN(nn1).addNN(nn2);
            MiniDFSNNTopology namenodeTopology = new MiniDFSNNTopology().addNameservice(nameservice);
            builder.nnTopology(namenodeTopology);
        }
        dfs = builder.build();
        // Configure contents of the filesystem
        org.apache.hadoop.fs.Path esUserPath = new org.apache.hadoop.fs.Path("/user/elasticsearch");
        FileSystem fs;
        if (isHA()) {
            dfs.transitionToActive(0);
            fs = HATestUtil.configureFailoverFs(dfs, cfg);
        } else {
            fs = dfs.getFileSystem(0);
        }

        try {
            // Set the elasticsearch user directory up
            fs.mkdirs(esUserPath);
            if (UserGroupInformation.isSecurityEnabled()) {
                List<AclEntry> acls = new ArrayList<>();
                acls.add(new AclEntry.Builder().setType(AclEntryType.USER).setName("elasticsearch").setPermission(FsAction.ALL).build());
                fs.modifyAclEntries(esUserPath, acls);
            }

            // Install a pre-existing repository into HDFS
            String directoryName = "readonly-repository";
            String archiveName = directoryName + ".tar.gz";
            URL readOnlyRepositoryArchiveURL = getClass().getClassLoader().getResource(archiveName);
            if (readOnlyRepositoryArchiveURL != null) {
                Path tempDirectory = Files.createTempDirectory(getClass().getName());
                File readOnlyRepositoryArchive = tempDirectory.resolve(archiveName).toFile();
                FileUtils.copyURLToFile(readOnlyRepositoryArchiveURL, readOnlyRepositoryArchive);
                FileUtil.unTar(readOnlyRepositoryArchive, tempDirectory.toFile());

                fs.copyFromLocalFile(
                    true,
                    true,
                    new org.apache.hadoop.fs.Path(tempDirectory.resolve(directoryName).toAbsolutePath().toUri()),
                    esUserPath.suffix("/existing/" + directoryName)
                );

                FileUtils.deleteDirectory(tempDirectory.toFile());
            }
        } finally {
            fs.close();
        }
    }

    private boolean isSecure() {
        return keytab != null && principalConfig != null;
    }

    @Override
    protected void after() {
        if (dfs != null) {
            try {
                if (isHA()) {
                    dfs.getFileSystem(0).close();
                    dfs.getFileSystem(1).close();
                } else {
                    dfs.getFileSystem().close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            dfs.close();
        }
        temporaryFolder.delete();
    }

    private boolean isHA() {
        return haNameService != null;
    }

    public int getPort() {
        return dfs == null ? explicitPort : dfs.getNameNodePort(0);
    }

    // fix port handling to allow parallel hdfs fixture runs
    public int getPort(int i) {
        return dfs.getNameNodePort(i);
    }

    /**
     * Wraps an HAServiceTarget, keeping track of any HAServiceProtocol proxies it generates in order
     * to close them at the end of the test lifecycle.
     */
    protected static class CloseableHAServiceTarget extends HAServiceTarget {
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
                if (protocol instanceof HAServiceProtocolClientSideTranslatorPB haServiceProtocolClientSideTranslatorPB) {
                    haServiceProtocolClientSideTranslatorPB.close();
                }
            }
        }
    }

    /**
     * The default HAAdmin tool does not throw exceptions on failures, and does not close any client connection
     * resources when it concludes. This subclass overrides the tool to allow for exception throwing, and to
     * keep track of and clean up connection resources.
     */
    protected static class CloseableHAAdmin extends DFSHAAdmin {
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void refreshKrb5Config() throws ClassNotFoundException, NoSuchMethodException, IllegalArgumentException,
        IllegalAccessException, InvocationTargetException, InvocationTargetException {
        Class classRef;
        if (System.getProperty("java.vendor").contains("IBM")) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }

        Method refreshMethod = classRef.getMethod("refresh");
        refreshMethod.invoke(classRef);
    }

    private static int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception ex) {
            LOGGER.error("Failed to find available port", ex);
        }
        return -1;
    }

}
