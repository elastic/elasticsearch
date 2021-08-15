/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import joptsimple.OptionSet;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.bouncycastle.asn1.DERIA5String;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.DLSequence;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.security.cli.HttpCertificateCommand.FileType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAKey;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.security.auth.x500.X500Principal;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.FileMatchers.isDirectory;
import static org.elasticsearch.test.FileMatchers.isRegularFile;
import static org.elasticsearch.test.FileMatchers.pathExists;
import static org.elasticsearch.xpack.security.cli.HttpCertificateCommand.guessFileType;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class HttpCertificateCommandTests extends ESTestCase {
    private static final String CA_PASSWORD = "ca-password";
    private FileSystem jimfs;
    private Path testRoot;

    @Before
    public void createTestDir() throws Exception {
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews("posix").build();
        jimfs = Jimfs.newFileSystem(conf);
        testRoot = jimfs.getPath(getClass().getSimpleName() + "-" + getTestName());
        IOUtils.rm(testRoot);
        Files.createDirectories(testRoot);
    }

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
    }

    @BeforeClass
    public static void muteOnBrokenJdk() {
        assumeFalse("JDK bug JDK-8266279, https://github.com/elastic/elasticsearch/issues/72359",
            "1.8.0_292".equals(System.getProperty("java.version")));
    }

    public void testGenerateSingleCertificateSigningRequest() throws Exception {
        final Path outFile = testRoot.resolve("csr.zip").toAbsolutePath();

        final List<String> hostNames = randomHostNames();
        final List<String> ipAddresses = randomIpAddresses();
        final String certificateName = hostNames.get(0);

        final HttpCertificateCommand command = new PathAwareHttpCertificateCommand(outFile);

        final MockTerminal terminal = new MockTerminal();

        terminal.addTextInput("y"); // generate CSR

        terminal.addTextInput(randomBoolean() ? "n" : ""); // cert-per-node

        // enter hostnames
        hostNames.forEach(terminal::addTextInput);
        terminal.addTextInput(""); // end-of-hosts
        terminal.addTextInput(randomBoolean() ? "y" : ""); // yes, correct

        // enter ip names
        ipAddresses.forEach(terminal::addTextInput);
        terminal.addTextInput(""); // end-of-ips
        terminal.addTextInput(randomBoolean() ? "y" : ""); // yes, correct

        terminal.addTextInput(randomBoolean() ? "n" : ""); // don't change advanced settings

        final String password = randomPassword();
        terminal.addSecretInput(password);
        if ("".equals(password) == false) {
            terminal.addSecretInput(password);
        } // confirm

        terminal.addTextInput(outFile.toString());

        final Environment env = newEnvironment();
        final OptionSet options = command.getParser().parse(new String[0]);
        command.execute(terminal, options, env);

        Path zipRoot = getZipRoot(outFile);

        assertThat(zipRoot.resolve("elasticsearch"), isDirectory());

        final Path csrPath = zipRoot.resolve("elasticsearch/http-" + certificateName + ".csr");
        final PKCS10CertificationRequest csr = readPemObject(csrPath, "CERTIFICATE REQUEST", PKCS10CertificationRequest::new);

        final Path keyPath = zipRoot.resolve("elasticsearch/http-" + certificateName + ".key");
        final AtomicBoolean wasEncrypted = new AtomicBoolean(false);
        final PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, () -> {
            wasEncrypted.set(true);
            return password.toCharArray();
        });
        if ("".equals(password) == false) {
            assertTrue("Password should have been required to decrypted key", wasEncrypted.get());
        }

        final Path esReadmePath = zipRoot.resolve("elasticsearch/README.txt");
        assertThat(esReadmePath, isRegularFile());
        final String esReadme = new String(Files.readAllBytes(esReadmePath), StandardCharsets.UTF_8);

        final Path ymlPath = zipRoot.resolve("elasticsearch/sample-elasticsearch.yml");
        assertThat(ymlPath, isRegularFile());
        final String yml = new String(Files.readAllBytes(ymlPath), StandardCharsets.UTF_8);

        // Verify the CSR was built correctly
        verifyCertificationRequest(csr, certificateName, hostNames, ipAddresses);

        // Verify the key
        assertMatchingPair(getPublicKey(csr), privateKey);

        final String csrName = csrPath.getFileName().toString();
        final String crtName = csrName.substring(0, csrName.length() - 4) + ".crt";

        // Verify the README
        assertThat(esReadme, containsString(csrName));
        assertThat(esReadme, containsString(crtName));
        assertThat(esReadme, containsString(keyPath.getFileName().toString()));
        assertThat(esReadme, containsString(ymlPath.getFileName().toString()));
        if ("".equals(password) == false) {
            assertThat(esReadme, not(containsString(password)));
        }

        // Verify the yml
        assertThat(yml, not(containsString(csrName)));
        assertThat(yml, containsString(crtName));
        assertThat(yml, containsString(keyPath.getFileName().toString()));
        if ("".equals(password) == false) {
            assertThat(yml, not(containsString(password)));
        }

        // Should not be a CA directory in CSR mode
        assertThat(zipRoot.resolve("ca"), not(pathExists()));

        // No CA in CSR mode
        verifyKibanaDirectory(zipRoot, false,
            Collections.singletonList("Certificate Signing Request"),
            Stream.of(password, csrName)
            .filter(s -> "".equals(s) == false).collect(Collectors.toList()));
    }

    public void testGenerateSingleCertificateWithExistingCA() throws Exception {
        final Path outFile = testRoot.resolve("certs.zip").toAbsolutePath();

        final List<String> hostNames = randomHostNames();
        final List<String> ipAddresses = randomIpAddresses();
        final String certificateName = hostNames.get(0);

        final Path caCertPath = getDataPath("ca.crt");
        assertThat(caCertPath, isRegularFile());
        final Path caKeyPath = getDataPath("ca.key");
        assertThat(caKeyPath, isRegularFile());
        final String caPassword = CA_PASSWORD;

        final int years = randomIntBetween(1, 8);

        final HttpCertificateCommand command = new PathAwareHttpCertificateCommand(outFile);

        final MockTerminal terminal = new MockTerminal();

        terminal.addTextInput(randomBoolean() ? "n" : ""); // don't generate CSR
        terminal.addTextInput("y"); // existing CA

        // randomise between cert+key, key+cert, PKCS12 : the tool is smart enough to handle any of those.
        switch (randomFrom(FileType.PEM_CERT, FileType.PEM_KEY, FileType.PKCS12)) {
            case PEM_CERT:
                terminal.addTextInput(caCertPath.toAbsolutePath().toString());
                terminal.addTextInput(caKeyPath.toAbsolutePath().toString());
                break;
            case PEM_KEY:
                terminal.addTextInput(caKeyPath.toAbsolutePath().toString());
                terminal.addTextInput(caCertPath.toAbsolutePath().toString());
                break;
            case PKCS12:
                terminal.addTextInput(getDataPath("ca.p12").toAbsolutePath().toString());
                break;
        }
        terminal.addSecretInput(caPassword);

        terminal.addTextInput(years + "y"); // validity period

        terminal.addTextInput(randomBoolean() ? "n" : ""); // don't use cert-per-node

        // enter hostnames
        hostNames.forEach(terminal::addTextInput);
        terminal.addTextInput(""); // end-of-hosts
        terminal.addTextInput(randomBoolean() ? "y" : ""); // yes, correct

        // enter ip names
        ipAddresses.forEach(terminal::addTextInput);
        terminal.addTextInput(""); // end-of-ips
        terminal.addTextInput(randomBoolean() ? "y" : ""); // yes, correct

        terminal.addTextInput(randomBoolean() ? "n" : ""); // don't change advanced settings

        final String password = randomPassword();
        terminal.addSecretInput(password);
        if ("".equals(password) == false) {
            terminal.addSecretInput(password);
        } // confirm

        terminal.addTextInput(outFile.toString());

        final Environment env = newEnvironment();
        final OptionSet options = command.getParser().parse(new String[0]);
        command.execute(terminal, options, env);

        Path zipRoot = getZipRoot(outFile);

        assertThat(zipRoot.resolve("elasticsearch"), isDirectory());

        final Path p12Path = zipRoot.resolve("elasticsearch/http.p12");

        final Path readmePath = zipRoot.resolve("elasticsearch/README.txt");
        assertThat(readmePath, isRegularFile());
        final String readme = new String(Files.readAllBytes(readmePath), StandardCharsets.UTF_8);

        final Path ymlPath = zipRoot.resolve("elasticsearch/sample-elasticsearch.yml");
        assertThat(ymlPath, isRegularFile());
        final String yml = new String(Files.readAllBytes(ymlPath), StandardCharsets.UTF_8);

        final Tuple<X509Certificate, PrivateKey> certAndKey = readCertificateAndKey(p12Path, password.toCharArray());

        // Verify the Cert was built correctly
        verifyCertificate(certAndKey.v1(), certificateName, years, hostNames, ipAddresses);
        assertThat(getRSAKeySize(certAndKey.v1().getPublicKey()), is(HttpCertificateCommand.DEFAULT_CERT_KEY_SIZE));
        assertThat(getRSAKeySize(certAndKey.v2()), is(HttpCertificateCommand.DEFAULT_CERT_KEY_SIZE));

        final X509Certificate caCert = readPemCertificate(caCertPath);
        verifyChain(certAndKey.v1(), caCert);

        // Verify the README
        assertThat(readme, containsString(p12Path.getFileName().toString()));
        assertThat(readme, containsString(ymlPath.getFileName().toString()));
        if ("".equals(password) == false) {
            assertThat(readme, not(containsString(password)));
        }
        assertThat(readme, not(containsString(caPassword)));

        // Verify the yml
        assertThat(yml, containsString(p12Path.getFileName().toString()));
        if ("".equals(password) == false) {
            assertThat(yml, not(containsString(password)));
        }
        assertThat(yml, not(containsString(caPassword)));

        // Should not be a CA directory when using an existing CA.
        assertThat(zipRoot.resolve("ca"), not(pathExists()));

        verifyKibanaDirectory(zipRoot, true, Collections.singletonList("2. elasticsearch-ca.pem"),
            Stream.of(password, caPassword, caKeyPath.getFileName().toString())
                .filter(s -> "".equals(s) == false).collect(Collectors.toList()));
    }

    public void testGenerateMultipleCertificateWithNewCA() throws Exception {
        final Path outFile = testRoot.resolve("certs.zip").toAbsolutePath();

        final int numberCerts = randomIntBetween(3, 6);
        final String[] certNames = new String[numberCerts];
        final String[] hostNames = new String[numberCerts];
        for (int i = 0; i < numberCerts; i++) {
            certNames[i] = randomAlphaOfLengthBetween(6, 12);
            hostNames[i] = randomAlphaOfLengthBetween(4, 8);
        }

        final HttpCertificateCommand command = new PathAwareHttpCertificateCommand(outFile);

        final MockTerminal terminal = new MockTerminal();

        terminal.addTextInput(randomBoolean() ? "n" : ""); // don't generate CSR
        terminal.addTextInput(randomBoolean() ? "n" : ""); // no existing CA

        final String caDN;
        final int caYears;
        final int caKeySize;
        // randomise whether to change CA defaults.
        if (randomBoolean()) {
            terminal.addTextInput("y"); // Change defaults
            caDN = "CN=" + randomAlphaOfLengthBetween(3, 8);
            caYears = randomIntBetween(1, 3);
            caKeySize = randomFrom(2048, 3072, 4096);
            terminal.addTextInput(caDN);
            terminal.addTextInput(caYears + "y");
            terminal.addTextInput(Integer.toString(caKeySize));
            terminal.addTextInput("n"); // Don't change values
        } else {
            terminal.addTextInput(randomBoolean() ? "n" : ""); // Don't change defaults
            caDN = HttpCertificateCommand.DEFAULT_CA_NAME.toString();
            caYears = HttpCertificateCommand.DEFAULT_CA_VALIDITY.getYears();
            caKeySize = HttpCertificateCommand.DEFAULT_CA_KEY_SIZE;
        }

        final String caPassword = randomPassword();
        terminal.addSecretInput(caPassword);
        if ("".equals(caPassword) == false) {
            terminal.addSecretInput(caPassword);
        } // confirm

        final int certYears = randomIntBetween(1, 8);
        terminal.addTextInput(certYears + "y"); // node cert validity period

        terminal.addTextInput("y"); // cert-per-node

        for (int i = 0; i < numberCerts; i++) {
            if (i != 0) {
                terminal.addTextInput(randomBoolean() ? "y" : ""); // another cert
            }

            // certificate / node name
            terminal.addTextInput(certNames[i]);

            // enter hostname
            terminal.addTextInput(hostNames[i]); // end-of-hosts
            terminal.addTextInput(""); // end-of-hosts
            terminal.addTextInput(randomBoolean() ? "y" : ""); // yes, correct

            // no ip
            terminal.addTextInput(""); // end-of-ip
            terminal.addTextInput(randomBoolean() ? "y" : ""); // yes, correct

            terminal.addTextInput(randomBoolean() ? "n" : ""); // don't change advanced settings
        }
        terminal.addTextInput("n"); // no more certs


        final String password = randomPassword();
        terminal.addSecretInput(password);
        if ("".equals(password) == false) {
            terminal.addSecretInput(password);
        } // confirm

        terminal.addTextInput(outFile.toString());

        final Environment env = newEnvironment();
        final OptionSet options = command.getParser().parse(new String[0]);
        command.execute(terminal, options, env);

        Path zipRoot = getZipRoot(outFile);

        // Should have a CA directory with the generated CA.
        assertThat(zipRoot.resolve("ca"), isDirectory());
        final Path caPath = zipRoot.resolve("ca/ca.p12");
        final Tuple<X509Certificate, PrivateKey> caCertKey = readCertificateAndKey(caPath, caPassword.toCharArray());
        verifyCertificate(caCertKey.v1(), caDN.replaceFirst("CN=", ""), caYears, Collections.emptyList(), Collections.emptyList());
        assertThat(getRSAKeySize(caCertKey.v1().getPublicKey()), is(caKeySize));
        assertThat(getRSAKeySize(caCertKey.v2()), is(caKeySize));

        assertThat(zipRoot.resolve("elasticsearch"), isDirectory());

        for (int i = 0; i < numberCerts; i++) {
            assertThat(zipRoot.resolve("elasticsearch/" + certNames[i]), isDirectory());
            final Path p12Path = zipRoot.resolve("elasticsearch/" + certNames[i] + "/http.p12");
            assertThat(p12Path, isRegularFile());

            final Path readmePath = zipRoot.resolve("elasticsearch/" + certNames[i] + "/README.txt");
            assertThat(readmePath, isRegularFile());
            final String readme = new String(Files.readAllBytes(readmePath), StandardCharsets.UTF_8);

            final Path ymlPath = zipRoot.resolve("elasticsearch/" + certNames[i] + "/sample-elasticsearch.yml");
            assertThat(ymlPath, isRegularFile());
            final String yml = new String(Files.readAllBytes(ymlPath), StandardCharsets.UTF_8);

            final Tuple<X509Certificate, PrivateKey> certAndKey = readCertificateAndKey(p12Path, password.toCharArray());

            // Verify the Cert was built correctly
            verifyCertificate(certAndKey.v1(), certNames[i], certYears, Collections.singletonList(hostNames[i]), Collections.emptyList());
            verifyChain(certAndKey.v1(), caCertKey.v1());
            assertThat(getRSAKeySize(certAndKey.v1().getPublicKey()), is(HttpCertificateCommand.DEFAULT_CERT_KEY_SIZE));
            assertThat(getRSAKeySize(certAndKey.v2()), is(HttpCertificateCommand.DEFAULT_CERT_KEY_SIZE));

            // Verify the README
            assertThat(readme, containsString(p12Path.getFileName().toString()));
            assertThat(readme, containsString(ymlPath.getFileName().toString()));
            if ("".equals(password) == false) {
                assertThat(readme, not(containsString(password)));
            }
            if ("".equals(caPassword) == false) {
                assertThat(readme, not(containsString(caPassword)));
            }

            // Verify the yml
            assertThat(yml, containsString(p12Path.getFileName().toString()));
            if ("".equals(password) == false) {
                assertThat(yml, not(containsString(password)));
            }
            if ("".equals(caPassword) == false) {
                assertThat(yml, not(containsString(caPassword)));
            }
        }

        verifyKibanaDirectory(zipRoot, true, Collections.singletonList("2. elasticsearch-ca.pem"),
            Stream.of(password, caPassword, caPath.getFileName().toString())
                .filter(s -> "".equals(s) == false).collect(Collectors.toList()));
    }

    public void testParsingValidityPeriod() throws Exception {
        final HttpCertificateCommand command = new HttpCertificateCommand();
        final MockTerminal terminal = new MockTerminal();

        terminal.addTextInput("2y");
        assertThat(command.readPeriodInput(terminal, "", null, 1), is(Period.ofYears(2)));

        terminal.addTextInput("18m");
        assertThat(command.readPeriodInput(terminal, "", null, 1), is(Period.ofMonths(18)));

        terminal.addTextInput("90d");
        assertThat(command.readPeriodInput(terminal, "", null, 1), is(Period.ofDays(90)));

        terminal.addTextInput("1y, 6m");
        assertThat(command.readPeriodInput(terminal, "", null, 1), is(Period.ofYears(1).withMonths(6)));

        // Test: Re-prompt on bad input.
        terminal.addTextInput("2m & 4d");
        terminal.addTextInput("2m 4d");
        assertThat(command.readPeriodInput(terminal, "", null, 1), is(Period.ofMonths(2).withDays(4)));

        terminal.addTextInput("1y, 6m");
        assertThat(command.readPeriodInput(terminal, "", null, 1), is(Period.ofYears(1).withMonths(6)));

        // Test: Accept default value
        final Period p = Period.of(randomIntBetween(1, 5), randomIntBetween(0, 11), randomIntBetween(0, 30));
        terminal.addTextInput("");
        assertThat(command.readPeriodInput(terminal, "", p, 1), is(p));

        final int y = randomIntBetween(1, 5);
        final int m = randomIntBetween(1, 11);
        final int d = randomIntBetween(1, 30);
        terminal.addTextInput(y + "y " + m + "m " + d + "d");
        assertThat(command.readPeriodInput(terminal, "", null, 1), is(Period.of(y, m, d)));

        // Test: Minimum Days
        final int shortDays = randomIntBetween(1, 20);

        terminal.addTextInput(shortDays + "d");
        terminal.addTextInput("y"); // I'm sure
        assertThat(command.readPeriodInput(terminal, "", null, 21), is(Period.ofDays(shortDays)));

        terminal.addTextInput(shortDays + "d");
        terminal.addTextInput("n"); // I'm not sure
        terminal.addTextInput("30d");
        assertThat(command.readPeriodInput(terminal, "", null, 21), is(Period.ofDays(30)));

        terminal.addTextInput("2m");
        terminal.addTextInput("n"); // I'm not sure
        terminal.addTextInput("2y");
        assertThat(command.readPeriodInput(terminal, "", null, 90), is(Period.ofYears(2)));
    }

    public void testValidityPeriodToString() throws Exception {
        assertThat(HttpCertificateCommand.toString(Period.ofYears(2)), is("2y"));
        assertThat(HttpCertificateCommand.toString(Period.ofMonths(5)), is("5m"));
        assertThat(HttpCertificateCommand.toString(Period.ofDays(60)), is("60d"));
        assertThat(HttpCertificateCommand.toString(Period.ZERO), is("0d"));
        assertThat(HttpCertificateCommand.toString(null), is("N/A"));

        final int y = randomIntBetween(1, 5);
        final int m = randomIntBetween(1, 11);
        final int d = randomIntBetween(1, 30);
        assertThat(HttpCertificateCommand.toString(Period.of(y, m, d)), is(y + "y," + m + "m," + d + "d"));
    }

    public void testGuessFileType() throws Exception {
        MockTerminal terminal = new MockTerminal();

        final Path caCert = getDataPath("ca.crt");
        final Path caKey = getDataPath("ca.key");
        assertThat(guessFileType(caCert, terminal), is(FileType.PEM_CERT));
        assertThat(guessFileType(caKey, terminal), is(FileType.PEM_KEY));

        final Path certChain = testRoot.resolve("ca.pem");
        try (OutputStream out = Files.newOutputStream(certChain)) {
            Files.copy(getDataPath("testnode.crt"), out);
            Files.copy(caCert, out);
        }
        assertThat(guessFileType(certChain, terminal), is(FileType.PEM_CERT_CHAIN));

        final Path tmpP12 = testRoot.resolve("tmp.p12");
        assertThat(guessFileType(tmpP12, terminal), is(FileType.PKCS12));
        final Path tmpJks = testRoot.resolve("tmp.jks");
        assertThat(guessFileType(tmpJks, terminal), is(FileType.JKS));

        final Path tmpKeystore = testRoot.resolve("tmp.keystore");
        writeDummyKeystore(tmpKeystore, "PKCS12");
        assertThat(guessFileType(tmpKeystore, terminal), is(FileType.PKCS12));
        writeDummyKeystore(tmpKeystore, "jks");
        assertThat(guessFileType(tmpKeystore, terminal), is(FileType.JKS));
    }

    public void testTextFileSubstitutions() throws Exception {
        CheckedBiFunction<String, Map<String, String>, String, Exception> copy = (source, subs) -> {
            try (InputStream in = new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8));
                 StringWriter out = new StringWriter();
                 PrintWriter writer = new PrintWriter(out)) {
                HttpCertificateCommand.copyWithSubstitutions(in, writer, subs);
                return out.toString().replace("\r\n", "\n");
            }
        };
        assertThat(copy.apply("abc\n", Collections.emptyMap()), is("abc\n"));
        assertThat(copy.apply("${not_a_var}\n", Collections.emptyMap()), is("${not_a_var}\n"));
        assertThat(copy.apply("${var}\n", singletonMap("var", "xyz")), is("xyz\n"));
        assertThat(copy.apply("#if not\nbody\n#endif\n", Collections.emptyMap()), is(""));
        assertThat(copy.apply("#if blank\nbody\n#endif\n", singletonMap("blank", "")), is(""));
        assertThat(copy.apply("#if yes\nbody\n#endif\n", singletonMap("yes", "true")), is("body\n"));
        assertThat(copy.apply("#if yes\ntrue\n#else\nfalse\n#endif\n", singletonMap("yes", "*")), is("true\n"));
        assertThat(copy.apply("#if blank\ntrue\n#else\nfalse\n#endif\n", singletonMap("blank", "")), is("false\n"));
        assertThat(copy.apply("#if var\n--> ${var} <--\n#else\n(${var})\n#endif\n", singletonMap("var", "foo")), is("--> foo <--\n"));
    }

    private Path getZipRoot(Path outFile) throws IOException, URISyntaxException {
        assertThat(outFile, isRegularFile());

        FileSystem fileSystem = FileSystems.newFileSystem(new URI("jar:" + outFile.toUri()), Collections.emptyMap());
        return fileSystem.getPath("/");
    }

    private List<String> randomIpAddresses() throws UnknownHostException {
        final int ipCount = randomIntBetween(0, 3);
        final List<String> ipAddresses = new ArrayList<>(ipCount);
        for (int i = 0; i < ipCount; i++) {
            String ip = randomIpAddress();
            ipAddresses.add(ip);
        }
        return ipAddresses;
    }

    private String randomIpAddress() throws UnknownHostException {
        return formatIpAddress(randomByteArrayOfLength(4));
    }

    private String formatIpAddress(byte[] addr) throws UnknownHostException {
        return NetworkAddress.format(InetAddress.getByAddress(addr));
    }

    private List<String> randomHostNames() {
        final int hostCount = randomIntBetween(1, 5);
        final List<String> hostNames = new ArrayList<>(hostCount);
        for (int i = 0; i < hostCount; i++) {
            String host = String.join(".", randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
            if (i > 0 && randomBoolean()) {
                host = "*." + host;
            }
            hostNames.add(host);
        }
        return hostNames;
    }

    private String randomPassword() {
        // We want to assert that this password doesn't end up in any output files, so we need to make sure we
        // don't randomly generate a real word.
        return randomFrom(
            "",
            randomAlphaOfLength(4) + randomFrom('~', '*', '%', '$', '|') + randomAlphaOfLength(4)
        );
    }

    private void verifyCertificationRequest(PKCS10CertificationRequest csr, String certificateName, List<String> hostNames,
                                            List<String> ipAddresses) throws IOException {
        // We rebuild the DN from the encoding because BC uses openSSL style toString, but we use LDAP style.
        assertThat(new X500Principal(csr.getSubject().getEncoded()).toString(), is("CN=" + certificateName.replaceAll("\\.", ", DC=")));
        final Attribute[] extensionAttributes = csr.getAttributes(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest);
        assertThat(extensionAttributes, arrayWithSize(1));
        assertThat(extensionAttributes[0].getAttributeValues(), arrayWithSize(1));
        assertThat(extensionAttributes[0].getAttributeValues()[0], instanceOf(DLSequence.class));

        // We register 1 extension - the subject alternative names
        final Extensions extensions = Extensions.getInstance(extensionAttributes[0].getAttributeValues()[0]);
        assertThat(extensions, notNullValue());
        final GeneralNames names = GeneralNames.fromExtensions(extensions, Extension.subjectAlternativeName);
        assertThat(names.getNames(), arrayWithSize(hostNames.size() + ipAddresses.size()));
        for (GeneralName name : names.getNames()) {
            assertThat(name.getTagNo(), oneOf(GeneralName.dNSName, GeneralName.iPAddress));
            if (name.getTagNo() == GeneralName.dNSName) {
                final String dns = DERIA5String.getInstance(name.getName()).getString();
                assertThat(dns, in(hostNames));
            } else if (name.getTagNo() == GeneralName.iPAddress) {
                final String ip = formatIpAddress(DEROctetString.getInstance(name.getName()).getOctets());
                assertThat(ip, in(ipAddresses));
            }
        }
    }

    private void verifyCertificate(X509Certificate cert, String certificateName, int years,
                                   List<String> hostNames, List<String> ipAddresses) throws CertificateParsingException {
        assertThat(cert.getSubjectX500Principal().toString(), is("CN=" + certificateName.replaceAll("\\.", ", DC=")));
        final Collection<List<?>> san = cert.getSubjectAlternativeNames();
        final int expectedSanEntries = hostNames.size() + ipAddresses.size();
        if (expectedSanEntries > 0) {
            assertThat(san, hasSize(expectedSanEntries));
            for (List<?> name : san) {
                assertThat(name, hasSize(2));
                assertThat(name.get(0), Matchers.instanceOf(Integer.class));
                assertThat(name.get(1), Matchers.instanceOf(String.class));
                final Integer tag = (Integer) name.get(0);
                final String value = (String) name.get(1);
                assertThat(tag, oneOf(GeneralName.dNSName, GeneralName.iPAddress));
                if (tag.intValue() == GeneralName.dNSName) {
                    assertThat(value, in(hostNames));
                } else if (tag.intValue() == GeneralName.iPAddress) {
                    assertThat(value, in(ipAddresses));
                }
            }
        } else if (san != null) {
            assertThat(san, hasSize(0));
        }

        // We don't know exactly when the certificate was generated, but it should have been in the last 10 minutes
        long now = System.currentTimeMillis();
        long nowMinus10Minutes = now - TimeUnit.MINUTES.toMillis(10);
        assertThat(cert.getNotBefore().getTime(), Matchers.lessThanOrEqualTo(now));
        assertThat(cert.getNotBefore().getTime(), Matchers.greaterThanOrEqualTo(nowMinus10Minutes));

        final ZonedDateTime expiry = Instant.ofEpochMilli(cert.getNotBefore().getTime()).atZone(ZoneOffset.UTC).plusYears(years);
        assertThat(cert.getNotAfter().getTime(), is(expiry.toInstant().toEpochMilli()));
    }

    private void verifyChain(X509Certificate... chain) throws GeneralSecurityException {
        for (int i = 1; i < chain.length; i++) {
            assertThat(chain[i - 1].getIssuerX500Principal(), is(chain[i].getSubjectX500Principal()));
            chain[i - 1].verify(chain[i].getPublicKey());
        }
        final X509Certificate root = chain[chain.length - 1];
        assertThat(root.getIssuerX500Principal(), is(root.getSubjectX500Principal()));
    }

    /**
     * Checks that a public + private key are a matching pair.
     */
    private void assertMatchingPair(PublicKey publicKey, PrivateKey privateKey) throws GeneralSecurityException {
        final byte[] bytes = randomByteArrayOfLength(128);
        final Signature rsa = Signature.getInstance("SHA512withRSA");

        rsa.initSign(privateKey);
        rsa.update(bytes);
        final byte[] signature = rsa.sign();

        rsa.initVerify(publicKey);
        rsa.update(bytes);
        assertTrue("PublicKey and PrivateKey are not a matching pair", rsa.verify(signature));
    }

    private void verifyKibanaDirectory(Path zipRoot, boolean expectCAFile, Iterable<String> readmeShouldContain,
                                       Iterable<String> shouldNotContain) throws IOException {
        assertThat(zipRoot.resolve("kibana"), isDirectory());
        if (expectCAFile) {
            assertThat(zipRoot.resolve("kibana/elasticsearch-ca.pem"), isRegularFile());
        } else {
            assertThat(zipRoot.resolve("kibana/elasticsearch-ca.pem"), not(pathExists()));
        }

        final Path kibanaReadmePath = zipRoot.resolve("kibana/README.txt");
        assertThat(kibanaReadmePath, isRegularFile());
        final String kibanaReadme = new String(Files.readAllBytes(kibanaReadmePath), StandardCharsets.UTF_8);

        final Path kibanaYmlPath = zipRoot.resolve("kibana/sample-kibana.yml");
        assertThat(kibanaYmlPath, isRegularFile());
        final String kibanaYml = new String(Files.readAllBytes(kibanaYmlPath), StandardCharsets.UTF_8);

        assertThat(kibanaReadme, containsString(kibanaYmlPath.getFileName().toString()));
        assertThat(kibanaReadme, containsString("elasticsearch.hosts"));
        assertThat(kibanaReadme, containsString("https://"));
        assertThat(kibanaReadme, containsString("elasticsearch-ca.pem"));
        readmeShouldContain.forEach(s -> assertThat(kibanaReadme, containsString(s)));
        shouldNotContain.forEach(s -> assertThat(kibanaReadme, not(containsString(s))));

        assertThat(kibanaYml, containsString("elasticsearch.ssl.certificateAuthorities: [ \"config/elasticsearch-ca.pem\" ]"));
        assertThat(kibanaYml, containsString("https://"));
        shouldNotContain.forEach(s -> assertThat(kibanaYml, not(containsString(s))));
    }

    private PublicKey getPublicKey(PKCS10CertificationRequest pkcs) throws GeneralSecurityException {
        return new JcaPKCS10CertificationRequest(pkcs).getPublicKey();
    }

    private int getRSAKeySize(Key key) {
        assertThat(key, instanceOf(RSAKey.class));
        final RSAKey rsa = (RSAKey) key;
        return rsa.getModulus().bitLength();
    }

    private Tuple<X509Certificate, PrivateKey> readCertificateAndKey(Path pkcs12,
                                                                     char[] password) throws IOException, GeneralSecurityException {

        final Map<Certificate, Key> entries = CertParsingUtils.readPkcs12KeyPairs(pkcs12, password, alias -> password);
        assertThat(entries.entrySet(), Matchers.hasSize(1));

        Certificate cert = entries.keySet().iterator().next();
        Key key = entries.get(cert);

        assertThat(cert, instanceOf(X509Certificate.class));
        assertThat(key, instanceOf(PrivateKey.class));
        assertMatchingPair(cert.getPublicKey(), (PrivateKey) key);
        return new Tuple<>((X509Certificate) cert, (PrivateKey) key);
    }

    private X509Certificate readPemCertificate(Path caCertPath) throws CertificateException, IOException {
        final Certificate[] certificates = CertParsingUtils.readCertificates(Collections.singletonList(caCertPath));
        assertThat(certificates, arrayWithSize(1));
        final Certificate cert = certificates[0];
        assertThat(cert, instanceOf(X509Certificate.class));
        return (X509Certificate) cert;
    }

    private <T> T readPemObject(Path path, String expectedType,
                                CheckedFunction<? super byte[], T, IOException> factory) throws IOException {
        assertThat(path, isRegularFile());
        final PemReader csrReader = new PemReader(Files.newBufferedReader(path));
        final PemObject csrPem = csrReader.readPemObject();
        assertThat(csrPem.getType(), is(expectedType));
        return factory.apply(csrPem.getContent());
    }

    private void writeDummyKeystore(Path path, String type) throws GeneralSecurityException, IOException {
        Files.deleteIfExists(path);
        KeyStore ks = KeyStore.getInstance(type);
        ks.load(null);
        if (randomBoolean()) {
            final X509Certificate cert = readPemCertificate(getDataPath("ca.crt"));
            ks.setCertificateEntry(randomAlphaOfLength(4), cert);
        }
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, randomAlphaOfLength(8).toCharArray());
        }
    }

    /**
     * A special version of {@link HttpCertificateCommand} that can resolve input strings back to JIMFS paths
     */
    private class PathAwareHttpCertificateCommand extends HttpCertificateCommand {

        final Map<String, Path> paths;

        PathAwareHttpCertificateCommand(Path... configuredPaths) {
            paths = Stream.of(configuredPaths).collect(Collectors.toMap(Path::toString, Function.identity()));
        }

        @Override
        protected Path resolvePath(String name) {
            return Optional.ofNullable(this.paths.get(name)).orElseGet(() -> super.resolvePath(name));
        }
    }

}
