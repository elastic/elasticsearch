/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.kerby.kerberos.kerb.server.KdcConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.net.ServerSocketFactory;

/**
 * Utility wrapper around Apache {@link SimpleKdcServer} backed by Unboundid
 * {@link InMemoryDirectoryServer}.<br>
 * Starts in memory Ldap server and then uses it as backend for Kdc Server.
 */
public class SimpleKdcLdapServer {
    private static final Logger logger = LogManager.getLogger(SimpleKdcLdapServer.class);

    private Path workDir = null;
    private SimpleKdcServer simpleKdc;
    private InMemoryDirectoryServer ldapServer;

    // KDC properties
    private String transport = ESTestCase.randomFrom("TCP", "UDP");
    private int kdcPort = 0;
    private String host;
    private String realm;
    private boolean krb5DebugBackupConfigValue;

    // LDAP properties
    private String baseDn;
    private Path ldiff;
    private int ldapPort;

    /**
     * Constructor for SimpleKdcLdapServer, creates instance of Kdc server and ldap
     * backend server. Also initializes and starts them with provided configuration.
     * <p>
     * To stop the KDC and ldap server use {@link #stop()}
     *
     * @param workDir Base directory for server, used to locate kdc.conf,
     *            backend.conf and kdc.ldiff
     * @param orgName Org name for base dn
     * @param domainName domain name for base dn
     * @param ldiff for ldap directory.
     * @throws Exception when KDC or Ldap server initialization fails
     */
    public SimpleKdcLdapServer(final Path workDir, final String orgName, final String domainName, final Path ldiff) throws Exception {
        this.workDir = workDir;
        this.realm = domainName.toUpperCase(Locale.ROOT) + "." + orgName.toUpperCase(Locale.ROOT);
        this.baseDn = "dc=" + domainName + ",dc=" + orgName;
        this.ldiff = ldiff;
        this.krb5DebugBackupConfigValue = AccessController.doPrivileged(new PrivilegedExceptionAction<Boolean>() {
            @Override
            @SuppressForbidden(reason = "set or clear system property krb5 debug in kerberos tests")
            public Boolean run() throws Exception {
                boolean oldDebugSetting = Boolean.parseBoolean(System.getProperty("sun.security.krb5.debug"));
                System.setProperty("sun.security.krb5.debug", Boolean.TRUE.toString());
                return oldDebugSetting;
            }
        });

        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                if (ESTestCase.awaitBusy(() -> init()) == false) {
                    throw new IllegalStateException("could not initialize SimpleKdcLdapServer");
                }
                return null;
            }
        });
        logger.info("SimpleKdcLdapServer started.");
    }

    @SuppressForbidden(reason = "Uses Apache Kdc which requires usage of java.io.File in order to create a SimpleKdcServer")
    private boolean init() {
        boolean initialized = false;
        try {
            // start ldap server
            createLdapServiceAndStart();
            // create ldap backend conf
            createLdapBackendConf();
            // Kdc Server
            simpleKdc = new SimpleKdcServer(this.workDir.toFile(), new KrbConfig());
            prepareKdcServerAndStart();
            initialized = true;
        } catch (Exception e) {
            if (simpleKdc != null) {
                try {
                    simpleKdc.stop();
                } catch (KrbException krbException) {
                    logger.debug("error occurred while cleaning up after init failure for SimpleKdcLdapServer");
                }
            }
            if (ldapServer != null) {
                ldapServer.shutDown(true);
            }
            ldapPort = 0;
            kdcPort = 0;
            initialized = false;
        }
        return initialized;
    }

    private void createLdapServiceAndStart() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(baseDn);
        config.setSchema(null);
        ldapServer = new InMemoryDirectoryServer(config);
        ldapServer.importFromLDIF(true, this.ldiff.toString());
        ldapServer.startListening();
        ldapPort = ldapServer.getListenPort();
    }

    private void createLdapBackendConf() throws IOException {
        String backendConf = KdcConfigKey.KDC_IDENTITY_BACKEND.getPropertyKey()
                + " = org.apache.kerby.kerberos.kdc.identitybackend.LdapIdentityBackend\n" + "host=127.0.0.1\n" + "port=" + ldapPort + "\n"
                + "admin_dn=uid=admin,ou=system," + baseDn + "\n" + "admin_pw=secret\n" + "base_dn=" + baseDn;
        Files.write(this.workDir.resolve("backend.conf"), backendConf.getBytes(StandardCharsets.UTF_8));
        assert Files.exists(this.workDir.resolve("backend.conf"));
    }

    @SuppressForbidden(reason = "Uses Apache Kdc which requires usage of java.io.File in order to create a SimpleKdcServer")
    private void prepareKdcServerAndStart() throws Exception {
        // transport
        simpleKdc.setWorkDir(workDir.toFile());
        simpleKdc.setKdcHost(host);
        simpleKdc.setKdcRealm(realm);
        if (transport != null) {
            if (kdcPort == 0) {
                kdcPort = getServerPort(transport);
            }
            if (transport.trim().equalsIgnoreCase("TCP")) {
                simpleKdc.setKdcTcpPort(kdcPort);
                simpleKdc.setAllowUdp(false);
            } else if (transport.trim().equalsIgnoreCase("UDP")) {
                simpleKdc.setKdcUdpPort(kdcPort);
                simpleKdc.setAllowTcp(false);
            } else {
                throw new IllegalArgumentException("Invalid transport: " + transport);
            }
        } else {
            throw new IllegalArgumentException("Need to set transport!");
        }
        final TimeValue minimumTicketLifeTime = new TimeValue(1, TimeUnit.DAYS);
        final TimeValue maxRenewableLifeTime = new TimeValue(7, TimeUnit.DAYS);
        simpleKdc.getKdcConfig().setLong(KdcConfigKey.MINIMUM_TICKET_LIFETIME, minimumTicketLifeTime.getMillis());
        simpleKdc.getKdcConfig().setLong(KdcConfigKey.MAXIMUM_RENEWABLE_LIFETIME, maxRenewableLifeTime.getMillis());
        simpleKdc.init();
        simpleKdc.start();
    }

    public String getRealm() {
        return realm;
    }

    public int getLdapListenPort() {
        return ldapPort;
    }

    public int getKdcPort() {
        return kdcPort;
    }

    /**
     * Creates a principal in the KDC with the specified user and password.
     *
     * @param principal principal name, do not include the domain.
     * @param password password.
     * @throws Exception thrown if the principal could not be created.
     */
    public synchronized void createPrincipal(final String principal, final String password) throws Exception {
        simpleKdc.createPrincipal(principal, password);
    }

    /**
     * Creates multiple principals in the KDC and adds them to a keytab file.
     *
     * @param keytabFile keytab file to add the created principals. If keytab file
     *            exists and then always appends to it.
     * @param principals principals to add to the KDC, do not include the domain.
     * @throws Exception thrown if the principals or the keytab file could not be
     *             created.
     */
    @SuppressForbidden(reason = "Uses Apache Kdc which requires usage of java.io.File in order to create a SimpleKdcServer")
    public synchronized void createPrincipal(final Path keytabFile, final String... principals) throws Exception {
        simpleKdc.createPrincipals(principals);
        for (String principal : principals) {
            simpleKdc.getKadmin().exportKeytab(keytabFile.toFile(), principal);
        }
    }

    /**
     * Stop Simple Kdc Server
     * 
     * @throws PrivilegedActionException when privileged action threw exception
     */
    public synchronized void stop() throws PrivilegedActionException {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {

            @Override
            @SuppressForbidden(reason = "set or clear system property krb5 debug in kerberos tests")
            public Void run() throws Exception {
                if (simpleKdc != null) {
                    try {
                        simpleKdc.stop();
                    } catch (KrbException e) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    } finally {
                        System.setProperty("sun.security.krb5.debug", Boolean.toString(krb5DebugBackupConfigValue));
                    }
                }

                if (ldapServer != null) {
                    ldapServer.shutDown(true);
                }
                return null;
            }
        });
        logger.info("SimpleKdcServer stoppped.");
    }

    private static int getServerPort(String transport) {
        if (transport != null && transport.trim().equalsIgnoreCase("TCP")) {
            try (ServerSocket serverSocket = ServerSocketFactory.getDefault().createServerSocket(0, 1,
                    InetAddress.getByName("127.0.0.1"))) {
                serverSocket.setReuseAddress(true);
                return serverSocket.getLocalPort();
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get a TCP server socket point");
            }
        } else if (transport != null && transport.trim().equalsIgnoreCase("UDP")) {
            try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getByName("127.0.0.1"))) {
                socket.setReuseAddress(true);
                return socket.getLocalPort();
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get a UDP server socket point");
            }
        }
        throw new IllegalArgumentException("Invalid transport: " + transport);
    }
}
