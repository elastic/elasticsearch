/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.ssl.CertUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tests the use of DNS only certificates with SSL and verifies hostname verification works. The test itself is simple since we just need
 * to check the cluster is formed and green. The setup is a bit more complicated as we do our best to ensure no issues will be encountered
 * with DNS.
 */
public class DNSOnlyHostnameVerificationTests extends SecurityIntegTestCase {

    private static KeyStore keystore = null;
    private static String hostName = null;

    @BeforeClass
    public static void resolveNameForMachine() throws Exception {
        assert keystore == null : "keystore is only set by this method and it should only be called once";
        NetworkService networkService = new NetworkService(Collections.emptyList());
        InetAddress inetAddress = networkService.resolvePublishHostAddresses(null);
        hostName = getHostName(inetAddress);
        String hostAddress = NetworkAddress.format(inetAddress);
        assumeFalse("need a local address that can be reverse resolved", hostName.equals(hostAddress));
        // looks good so far, verify forward resolve is ok and proceed
        Optional<InetAddress> matchingForwardResolvedAddress = Arrays.stream(InetAddress.getAllByName(hostName))
                    .filter((i) -> Arrays.equals(i.getAddress(), inetAddress.getAddress()))
                    .findFirst();
        assumeTrue("could not forward resolve hostname: " + hostName, matchingForwardResolvedAddress.isPresent());
        KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();

        // randomize between CN and SAN
        final X509Certificate cert;
        if (randomBoolean()) {
            cert = CertUtils.generateSignedCertificate(new X500Principal("CN=" + hostName), null, keyPair, null, null, 365);
        } else {
            GeneralName dnsSan = new GeneralName(GeneralName.dNSName, hostName);
            GeneralNames names = new GeneralNames(dnsSan);
            cert = CertUtils.generateSignedCertificate(new X500Principal("CN=esnode"), names, keyPair, null, null, 365);
        }

        keystore = KeyStore.getInstance("JKS");
        keystore.load(null, null);
        keystore.setKeyEntry("private key", keyPair.getPrivate(), SecuritySettingsSource.TEST_PASSWORD.toCharArray(),
                new Certificate[]{cert});
    }

    @AfterClass
    public static void cleanupKeystore() {
        keystore = null;
        hostName = null;
    }

    @Override
    public boolean useGeneratedSSLConfig() {
        return false;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        Settings defaultSettings = super.nodeSettings(nodeOrdinal);
        Settings.Builder builder = Settings.builder()
                .put(defaultSettings.filter((s) -> s.startsWith("xpack.ssl.") == false).getAsMap())
                .put("transport.host", hostName);
        Path keystorePath = nodeConfigPath(nodeOrdinal).resolve("keystore.jks");
        try (OutputStream os = Files.newOutputStream(keystorePath)) {
            keystore.store(os, SecuritySettingsSource.TEST_PASSWORD.toCharArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
            throw new ElasticsearchException("unable to write keystore for node", e);
        }
        SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
            secureSettings.setString("xpack.ssl.keystore.secure_password", SecuritySettingsSource.TEST_PASSWORD);
            secureSettings.setString("xpack.ssl.truststore.secure_password", SecuritySettingsSource.TEST_PASSWORD);
        });
        builder.put("xpack.ssl.keystore.path", keystorePath.toAbsolutePath())
               .put("xpack.ssl.truststore.path", keystorePath.toAbsolutePath());
        List<String> unicastHosts = Arrays.stream(defaultSettings.getAsArray("discovery.zen.ping.unicast.hosts"))
                .map((s) -> {
                    String port = s.substring(s.lastIndexOf(':'), s.length());
                    return hostName + port;
                })
                .collect(Collectors.toList());
        builder.putArray("discovery.zen.ping.unicast.hosts", unicastHosts);
        return builder.build();
    }

    @Override
    public Settings transportClientSettings() {
        Settings defaultSettings = super.transportClientSettings();
        Settings.Builder builder = Settings.builder()
                .put(defaultSettings.filter((s) -> s.startsWith("xpack.ssl.") == false));
        Path path = createTempDir().resolve("keystore.jks");
        try (OutputStream os = Files.newOutputStream(path)) {
            keystore.store(os, SecuritySettingsSource.TEST_PASSWORD.toCharArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
            throw new ElasticsearchException("unable to write keystore for node", e);
        }
        SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
            secureSettings.setString("xpack.ssl.keystore.secure_password", SecuritySettingsSource.TEST_PASSWORD);
            secureSettings.setString("xpack.ssl.truststore.secure_password", SecuritySettingsSource.TEST_PASSWORD);
        });
        builder.put("xpack.ssl.keystore.path", path.toAbsolutePath())
               .put("xpack.ssl.truststore.path", path.toAbsolutePath());
        return builder.build();
    }

    public void testThatClusterIsFormed() {
        ensureGreen();
    }

    @SuppressForbidden(reason = "need to get the hostname to set as host in test")
    private static String getHostName(InetAddress inetAddress) {
        return inetAddress.getHostName();
    }
}
