/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.lucene.util.IOUtils;
import org.bouncycastle.asn1.ASN1String;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.ssl.CertificateTool.CAInfo;
import org.elasticsearch.xpack.ssl.CertificateTool.CertificateInformation;
import org.elasticsearch.xpack.ssl.CertificateTool.Name;
import org.junit.After;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAKey;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for the tool used to simplify SSL certificate generation
 */
public class CertificateToolTests extends ESTestCase {

    private FileSystem jimfs;

    private Path initTempDir() throws Exception {
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews("posix").build();
        jimfs = Jimfs.newFileSystem(conf);
        Path tempDir = jimfs.getPath("temp");
        IOUtils.rm(tempDir);
        Files.createDirectories(tempDir);
        return tempDir;
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.close(jimfs);
        super.tearDown();
    }

    public void testOutputDirectory() throws Exception {
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        Path outputDir = createTempDir();
        Path outputFile = outputDir.resolve("certs.zip");
        MockTerminal terminal = new MockTerminal();

        // test with a user provided dir
        Path resolvedOutputFile = CertificateTool.getOutputFile(terminal, outputFile.toString(), env, null);
        assertEquals(outputFile, resolvedOutputFile);
        assertTrue(terminal.getOutput().isEmpty());

        // test without a user provided directory
        Path userPromptedOutputFile = outputDir.resolve("csr");
        assertFalse(Files.exists(userPromptedOutputFile));
        terminal.addTextInput(userPromptedOutputFile.toString());
        resolvedOutputFile = CertificateTool.getOutputFile(terminal, null, env, "out.zip");
        assertEquals(userPromptedOutputFile, resolvedOutputFile);
        assertTrue(terminal.getOutput().isEmpty());

        // test with empty user input
        String defaultFilename = randomAsciiOfLengthBetween(1, 10);
        Path expectedDefaultPath = XPackPlugin.resolveConfigFile(env, defaultFilename);
        terminal.addTextInput("");
        resolvedOutputFile = CertificateTool.getOutputFile(terminal, null, env, defaultFilename);
        assertEquals(expectedDefaultPath, resolvedOutputFile);
        assertTrue(terminal.getOutput().isEmpty());
    }

    public void testPromptingForInstanceInformation() throws Exception {
        final int numberOfInstances = scaledRandomIntBetween(1, 12);
        Map<String, Map<String, String>> instanceInput = new HashMap<>(numberOfInstances);
        for (int i = 0; i < numberOfInstances; i++) {
            final String name;
            while (true) {
                String randomName = getValidRandomInstanceName();
                if (instanceInput.containsKey(randomName) == false) {
                    name = randomName;
                    break;
                }
            }
            Map<String, String> instanceInfo = new HashMap<>();
            instanceInput.put(name, instanceInfo);
            instanceInfo.put("ip", randomFrom("127.0.0.1", "::1", "192.168.1.1,::1", ""));
            instanceInfo.put("dns", randomFrom("localhost", "localhost.localdomain", "localhost,myhost", ""));
            logger.info("instance [{}] name [{}] [{}]", i, name, instanceInfo);
        }

        int count = 0;
        MockTerminal terminal = new MockTerminal();
        for (Entry<String, Map<String, String>> entry : instanceInput.entrySet()) {
            terminal.addTextInput(entry.getKey());
            terminal.addTextInput("");
            terminal.addTextInput(entry.getValue().get("ip"));
            terminal.addTextInput(entry.getValue().get("dns"));
            count++;
            if (count == numberOfInstances) {
                terminal.addTextInput("n");
            } else {
                terminal.addTextInput("y");
            }
        }

        Collection<CertificateInformation> certInfos = CertificateTool.getCertificateInformationList(terminal, null,
                new Environment(Settings.builder().put("path.home", createTempDir()).build()));
        logger.info("certificate tool output:\n{}", terminal.getOutput());
        assertEquals(numberOfInstances, certInfos.size());
        for (CertificateInformation certInfo : certInfos) {
            String name = certInfo.name.originalName;
            Map<String, String> instanceInfo = instanceInput.get(name);
            assertNotNull("did not find map for " + name, instanceInfo);
            List<String> expectedIps = Arrays.asList(Strings.commaDelimitedListToStringArray(instanceInfo.get("ip")));
            List<String> expectedDns = Arrays.asList(Strings.commaDelimitedListToStringArray(instanceInfo.get("dns")));
            assertEquals(expectedIps, certInfo.ipAddresses);
            assertEquals(expectedDns, certInfo.dnsNames);
            instanceInput.remove(name);
        }
        assertEquals(0, instanceInput.size());
        final String output = terminal.getOutput();
        assertTrue("Output: " + output, output.isEmpty());
    }

    public void testParsingFile() throws Exception {
        Path tempDir = initTempDir();
        Path instanceFile = writeInstancesTo(tempDir.resolve("instances.yml"));
        Collection<CertificateInformation> certInfos = CertificateTool.parseFile(instanceFile);
        assertEquals(4, certInfos.size());

        Map<String, CertificateInformation> certInfosMap =
                certInfos.stream().collect(Collectors.toMap((c) -> c.name.originalName, Function.identity()));
        CertificateInformation certInfo = certInfosMap.get("node1");
        assertEquals(Collections.singletonList("127.0.0.1"), certInfo.ipAddresses);
        assertEquals(Collections.singletonList("localhost"), certInfo.dnsNames);
        assertEquals("node1", certInfo.name.filename);

        certInfo = certInfosMap.get("node2");
        assertEquals(Collections.singletonList("::1"), certInfo.ipAddresses);
        assertEquals(Collections.emptyList(), certInfo.dnsNames);
        assertEquals("node2", certInfo.name.filename);

        certInfo = certInfosMap.get("node3");
        assertEquals(Collections.emptyList(), certInfo.ipAddresses);
        assertEquals(Collections.emptyList(), certInfo.dnsNames);
        assertEquals("node3", certInfo.name.filename);

        certInfo = certInfosMap.get("CN=different value");
        assertEquals(Collections.emptyList(), certInfo.ipAddresses);
        assertEquals(Collections.singletonList("node4.mydomain.com"), certInfo.dnsNames);
        assertEquals("different file", certInfo.name.filename);
    }

    public void testGeneratingCsr() throws Exception {
        Path tempDir = initTempDir();
        Path outputFile = tempDir.resolve("out.zip");
        Path instanceFile = writeInstancesTo(tempDir.resolve("instances.yml"));
        Collection<CertificateInformation> certInfos = CertificateTool.parseFile(instanceFile);
        assertEquals(4, certInfos.size());

        assertFalse(Files.exists(outputFile));
        CertificateTool.generateAndWriteCsrs(outputFile, certInfos, randomFrom(1024, 2048));
        assertTrue(Files.exists(outputFile));

        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(outputFile);
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_READ));
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_WRITE));
        assertEquals(perms.toString(), 2, perms.size());

        FileSystem fileSystem = FileSystems.newFileSystem(new URI("jar:" + outputFile.toUri()), Collections.emptyMap());
        Path zipRoot = fileSystem.getPath("/");

        assertFalse(Files.exists(zipRoot.resolve("ca")));
        for (CertificateInformation certInfo : certInfos) {
            String filename = certInfo.name.filename;
            assertTrue(Files.exists(zipRoot.resolve(filename)));
            final Path csr = zipRoot.resolve(filename + "/" + filename + ".csr");
            assertTrue(Files.exists(csr));
            assertTrue(Files.exists(zipRoot.resolve(filename + "/" + filename + ".key")));
            PKCS10CertificationRequest request = readCertificateRequest(csr);
            assertEquals(certInfo.name.x500Principal.getName(), request.getSubject().toString());
            Attribute[] extensionsReq = request.getAttributes(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest);
            if (certInfo.ipAddresses.size() > 0 || certInfo.dnsNames.size() > 0) {
                assertEquals(1, extensionsReq.length);
                Extensions extensions = Extensions.getInstance(extensionsReq[0].getAttributeValues()[0]);
                GeneralNames subjAltNames = GeneralNames.fromExtensions(extensions, Extension.subjectAlternativeName);
                assertSubjAltNames(subjAltNames, certInfo);
            } else {
                assertEquals(0, extensionsReq.length);
            }
        }
    }

    public void testGeneratingSignedCertificates() throws Exception {
        Path tempDir = initTempDir();
        Path outputFile = tempDir.resolve("out.zip");
        Path instanceFile = writeInstancesTo(tempDir.resolve("instances.yml"));
        Collection<CertificateInformation> certInfos = CertificateTool.parseFile(instanceFile);
        assertEquals(4, certInfos.size());

        final int keysize = randomFrom(1024, 2048);
        final int days = randomIntBetween(1, 1024);
        KeyPair keyPair = CertUtils.generateKeyPair(keysize);
        X509Certificate caCert = CertUtils.generateCACertificate(new X500Principal("CN=test ca"), keyPair, days);

        final boolean generatedCa = randomBoolean();
        final char[] keyPassword = randomBoolean() ? "changeme".toCharArray() : null;
        assertFalse(Files.exists(outputFile));
        CAInfo caInfo = new CAInfo(caCert, keyPair.getPrivate(), generatedCa, keyPassword);
        CertificateTool.generateAndWriteSignedCertificates(outputFile, certInfos, caInfo, keysize, days);
        assertTrue(Files.exists(outputFile));

        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(outputFile);
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_READ));
        assertTrue(perms.toString(), perms.contains(PosixFilePermission.OWNER_WRITE));
        assertEquals(perms.toString(), 2, perms.size());

        FileSystem fileSystem = FileSystems.newFileSystem(new URI("jar:" + outputFile.toUri()), Collections.emptyMap());
        Path zipRoot = fileSystem.getPath("/");

        if (generatedCa) {
            assertTrue(Files.exists(zipRoot.resolve("ca")));
            assertTrue(Files.exists(zipRoot.resolve("ca").resolve("ca.crt")));
            assertTrue(Files.exists(zipRoot.resolve("ca").resolve("ca.key")));
            // check the CA cert
            try (Reader reader = Files.newBufferedReader(zipRoot.resolve("ca").resolve("ca.crt"))) {
                X509Certificate parsedCaCert = readX509Certificate(reader);
                assertThat(parsedCaCert.getSubjectX500Principal().getName(), containsString("test ca"));
                assertEquals(caCert, parsedCaCert);
                long daysBetween = ChronoUnit.DAYS.between(caCert.getNotBefore().toInstant(), caCert.getNotAfter().toInstant());
                assertEquals(days, (int) daysBetween);
            }

            // check the CA key
            if (keyPassword != null) {
                try (Reader reader = Files.newBufferedReader(zipRoot.resolve("ca").resolve("ca.key"))) {
                    PEMParser pemParser = new PEMParser(reader);
                    Object parsed = pemParser.readObject();
                    assertThat(parsed, instanceOf(PEMEncryptedKeyPair.class));
                    char[] zeroChars = new char[keyPassword.length];
                    Arrays.fill(zeroChars, (char) 0);
                    assertArrayEquals(zeroChars, keyPassword);
                }
            }

            try (Reader reader = Files.newBufferedReader(zipRoot.resolve("ca").resolve("ca.key"))) {
                PrivateKey privateKey = CertUtils.readPrivateKey(reader, () -> keyPassword != null ? "changeme".toCharArray() : null);
                assertEquals(caInfo.privateKey, privateKey);
            }
        } else {
            assertFalse(Files.exists(zipRoot.resolve("ca")));
        }

        for (CertificateInformation certInfo : certInfos) {
            String filename = certInfo.name.filename;
            assertTrue(Files.exists(zipRoot.resolve(filename)));
            final Path cert = zipRoot.resolve(filename + "/" + filename + ".crt");
            assertTrue(Files.exists(cert));
            assertTrue(Files.exists(zipRoot.resolve(filename + "/" + filename + ".key")));
            try (Reader reader = Files.newBufferedReader(cert)) {
                X509Certificate certificate = readX509Certificate(reader);
                assertEquals(certInfo.name.x500Principal.toString(), certificate.getSubjectX500Principal().getName());
                final int sanCount = certInfo.ipAddresses.size() + certInfo.dnsNames.size();
                if (sanCount == 0) {
                    assertNull(certificate.getSubjectAlternativeNames());
                } else {
                    X509CertificateHolder x509CertHolder = new X509CertificateHolder(certificate.getEncoded());
                    GeneralNames subjAltNames =
                            GeneralNames.fromExtensions(x509CertHolder.getExtensions(), Extension.subjectAlternativeName);
                    assertSubjAltNames(subjAltNames, certInfo);
                }
            }
        }
    }

    public void testGetCAInfo() throws Exception {
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        Path testNodeCertPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        Path testNodeKeyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");
        final boolean passwordPrompt = randomBoolean();
        MockTerminal terminal = new MockTerminal();
        if (passwordPrompt) {
            terminal.addSecretInput("testnode");
        }

        final int days = randomIntBetween(1, 1024);
        CAInfo caInfo = CertificateTool.getCAInfo(terminal, "CN=foo", testNodeCertPath.toString(), testNodeKeyPath.toString(),
                passwordPrompt ? null : "testnode".toCharArray(), passwordPrompt, env, randomFrom(1024, 2048), days);
        assertTrue(terminal.getOutput().isEmpty());
        assertEquals(caInfo.caCert.getSubjectX500Principal().getName(),
                "CN=Elasticsearch Test Node,OU=elasticsearch,O=org");
        assertThat(caInfo.privateKey.getAlgorithm(), containsString("RSA"));
        assertEquals(2048, ((RSAKey) caInfo.privateKey).getModulus().bitLength());
        assertFalse(caInfo.generated);
        long daysBetween = ChronoUnit.DAYS.between(caInfo.caCert.getNotBefore().toInstant(), caInfo.caCert.getNotAfter().toInstant());
        assertEquals(1460L, daysBetween);

        // test generation
        final boolean passwordProtected = randomBoolean();
        final char[] password;
        if (passwordPrompt && passwordProtected) {
            password = null;
            terminal.addSecretInput("testnode");
        } else {
            password = "testnode".toCharArray();
        }
        final int keysize = randomFrom(1024, 2048);
        caInfo = CertificateTool.getCAInfo(terminal, "CN=foo bar", null, null, password, passwordProtected && passwordPrompt, env,
                keysize, days);
        assertTrue(terminal.getOutput().isEmpty());
        assertThat(caInfo.caCert, instanceOf(X509Certificate.class));
        assertEquals(caInfo.caCert.getSubjectX500Principal().getName(), "CN=foo bar");
        assertThat(caInfo.privateKey.getAlgorithm(), containsString("RSA"));
        assertTrue(caInfo.generated);
        assertEquals(keysize, ((RSAKey) caInfo.privateKey).getModulus().bitLength());
        daysBetween = ChronoUnit.DAYS.between(caInfo.caCert.getNotBefore().toInstant(), caInfo.caCert.getNotAfter().toInstant());
        assertEquals(days, (int) daysBetween);
    }

    public void testNameValues() throws Exception {
        // good name
        Name name = Name.fromUserProvidedName("my instance", "my instance");
        assertEquals("my instance", name.originalName);
        assertNull(name.error);
        assertEquals("CN=my instance", name.x500Principal.getName());
        assertEquals("my instance", name.filename);

        // too long
        String userProvidedName = randomAsciiOfLength(CertificateTool.MAX_FILENAME_LENGTH + 1);
        name = Name.fromUserProvidedName(userProvidedName, userProvidedName);
        assertEquals(userProvidedName, name.originalName);
        assertThat(name.error, containsString("valid filename"));

        // too short
        name = Name.fromUserProvidedName("", "");
        assertEquals("", name.originalName);
        assertThat(name.error, containsString("valid filename"));
        assertEquals("CN=", name.x500Principal.getName());
        assertNull(name.filename);

        // invalid characters only
        userProvidedName = "<>|<>*|?\"\\";
        name = Name.fromUserProvidedName(userProvidedName, userProvidedName);
        assertEquals(userProvidedName, name.originalName);
        assertThat(name.error, containsString("valid DN"));
        assertNull(name.x500Principal);
        assertNull(name.filename);

        // invalid for file but DN ok
        userProvidedName = "*";
        name = Name.fromUserProvidedName(userProvidedName, userProvidedName);
        assertEquals(userProvidedName, name.originalName);
        assertThat(name.error, containsString("valid filename"));
        assertEquals("CN=" + userProvidedName, name.x500Principal.getName());
        assertNull(name.filename);

        // invalid with valid chars for filename
        userProvidedName = "*.mydomain.com";
        name = Name.fromUserProvidedName(userProvidedName, userProvidedName);
        assertEquals(userProvidedName, name.originalName);
        assertThat(name.error, containsString("valid filename"));
        assertEquals("CN=" + userProvidedName, name.x500Principal.getName());

        // valid but could create hidden file/dir so it is not allowed
        userProvidedName = ".mydomain.com";
        name = Name.fromUserProvidedName(userProvidedName, userProvidedName);
        assertEquals(userProvidedName, name.originalName);
        assertThat(name.error, containsString("valid filename"));
        assertEquals("CN=" + userProvidedName, name.x500Principal.getName());
    }

    private PKCS10CertificationRequest readCertificateRequest(Path path) throws Exception {
        try (Reader reader = Files.newBufferedReader(path);
             PEMParser pemParser = new PEMParser(reader)) {
            Object object = pemParser.readObject();
            assertThat(object, instanceOf(PKCS10CertificationRequest.class));
            return (PKCS10CertificationRequest) object;
        }
    }

    private X509Certificate readX509Certificate(Reader reader) throws Exception {
        List<Certificate> list = new ArrayList<>(1);
        CertUtils.readCertificates(reader, list, CertificateFactory.getInstance("X.509"));
        assertEquals(1, list.size());
        assertThat(list.get(0), instanceOf(X509Certificate.class));
        return (X509Certificate) list.get(0);
    }

    private void assertSubjAltNames(GeneralNames subjAltNames, CertificateInformation certInfo) throws Exception {
        assertEquals(certInfo.ipAddresses.size() + certInfo.dnsNames.size(), subjAltNames.getNames().length);
        Collections.sort(certInfo.dnsNames);
        Collections.sort(certInfo.ipAddresses);
        for (GeneralName generalName : subjAltNames.getNames()) {
            if (generalName.getTagNo() == GeneralName.dNSName) {
                String dns = ((ASN1String)generalName.getName()).getString();
                assertThat(Collections.binarySearch(certInfo.dnsNames, dns), greaterThanOrEqualTo(0));
            } else if (generalName.getTagNo() == GeneralName.iPAddress) {
                byte[] ipBytes = DEROctetString.getInstance(generalName.getName()).getOctets();
                String ip = NetworkAddress.format(InetAddress.getByAddress(ipBytes));
                assertThat(Collections.binarySearch(certInfo.ipAddresses, ip), greaterThanOrEqualTo(0));
            } else {
                fail("unknown general name with tag " + generalName.getTagNo());
            }
        }
    }

    /**
     * Gets a random name that is valid for certificate generation. There are some cases where the random value could match one of the
     * reserved names like ca, so this method allows us to avoid these issues.
     */
    private String getValidRandomInstanceName() {
        String name;
        boolean valid;
        do {
            name = randomAsciiOfLengthBetween(1, 32);
            valid = Name.fromUserProvidedName(name, name).error == null;
        } while (valid == false);
        return name;
    }

    /**
     * Writes the description of instances to a given {@link Path}
     */
    private Path writeInstancesTo(Path path) throws IOException {
        Iterable<String> instances = Arrays.asList(
                "instances:",
                "  - name: \"node1\"",
                "    ip:",
                "      - \"127.0.0.1\"",
                "    dns: \"localhost\"",
                "  - name: \"node2\"",
                "    filename: \"node2\"",
                "    ip: \"::1\"",
                "  - name: \"node3\"",
                "    filename: \"node3\"",
                "  - name: \"CN=different value\"",
                "    filename: \"different file\"",
                "    dns:",
                "      - \"node4.mydomain.com\"");

        return Files.write(path, instances, StandardCharsets.UTF_8);
    }
}
