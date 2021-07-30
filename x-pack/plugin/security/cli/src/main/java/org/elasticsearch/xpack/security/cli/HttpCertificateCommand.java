/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.bouncycastle.asn1.DERIA5String;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.OperatorException;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.util.io.pem.PemObjectGenerator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.elasticsearch.xpack.security.cli.CertGenUtils.generateSignedCertificate;

/**
 * This command is the "elasticsearch-certutil http" command. It provides a guided process for creating
 * certificates or CSRs for the Rest (http/s) interface of Elasticsearch and configuring other stack products
 * to trust this certificate.
 */
class HttpCertificateCommand extends EnvironmentAwareCommand {

    static final int DEFAULT_CERT_KEY_SIZE = 2048;
    static final Period DEFAULT_CERT_VALIDITY = Period.ofYears(5);

    static final X500Principal DEFAULT_CA_NAME = new X500Principal("CN=Elasticsearch HTTP CA");
    static final int DEFAULT_CA_KEY_SIZE = DEFAULT_CERT_KEY_SIZE;
    static final Period DEFAULT_CA_VALIDITY = DEFAULT_CERT_VALIDITY;

    private static final String ES_README_CSR = "es-readme-csr.txt";
    private static final String ES_YML_CSR = "es-sample-csr.yml";
    private static final String ES_README_P12 = "es-readme-p12.txt";
    private static final String ES_YML_P12 = "es-sample-p12.yml";
    private static final String CA_README_P12 = "ca-readme-p12.txt";
    private static final String KIBANA_README = "kibana-readme.txt";
    private static final String KIBANA_YML = "kibana-sample.yml";

    /**
     * Magic bytes for a non-empty PKCS#12 file
     */
    private static final byte[] MAGIC_BYTES1_PKCS12 = new byte[] { (byte) 0x30, (byte) 0x82 };
    /**
     * Magic bytes for an empty PKCS#12 file
     */
    private static final byte[] MAGIC_BYTES2_PKCS12 = new byte[] { (byte) 0x30, (byte) 0x56 };
    private static final byte[] MAGIC_BYTES2_JDK16_PKCS12 = new byte[] { (byte) 0x30, (byte) 0x65 };
    /**
     * Magic bytes for a JKS keystore
     */
    private static final byte[] MAGIC_BYTES_JKS = new byte[] { (byte) 0xFE, (byte) 0xED };

    enum FileType {
        PKCS12,
        JKS,
        PEM_CERT,
        PEM_KEY,
        PEM_CERT_CHAIN,
        UNRECOGNIZED;
    }

    private class CertOptions {
        final String name;
        final X500Principal subject;
        final List<String> dnsNames;
        final List<String> ipNames;
        final int keySize;
        final Period validity;

        private CertOptions(String name, X500Principal subject, List<String> dnsNames, List<String> ipNames, int keySize, Period validity) {
            this.name = name;
            this.subject = subject;
            this.dnsNames = dnsNames;
            this.ipNames = ipNames;
            this.keySize = keySize;
            this.validity = validity;
        }
    }

    HttpCertificateCommand() {
        super("generate a new certificate (or certificate request) for the Elasticsearch HTTP interface");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        printHeader("Elasticsearch HTTP Certificate Utility", terminal);

        terminal.println("The 'http' command guides you through the process of generating certificates");
        terminal.println("for use on the HTTP (Rest) interface for Elasticsearch.");
        terminal.println("");
        terminal.println("This tool will ask you a number of questions in order to generate the right");
        terminal.println("set of files for your needs.");

        final CertificateTool.CAInfo caInfo;
        final Period validity;
        final boolean csr = askCertSigningRequest(terminal);
        if (csr) {
            caInfo = null;
            validity = null;
        } else {
            final boolean existingCa = askExistingCertificateAuthority(terminal);
            if (existingCa) {
                caInfo = findExistingCA(terminal, env);
            } else {
                caInfo = createNewCA(terminal);
            }
            terminal.println(Terminal.Verbosity.VERBOSE, "Using the following CA:");
            terminal.println(Terminal.Verbosity.VERBOSE, "\tSubject: " + caInfo.certAndKey.cert.getSubjectX500Principal());
            terminal.println(Terminal.Verbosity.VERBOSE, "\tIssuer: " + caInfo.certAndKey.cert.getIssuerX500Principal());
            terminal.println(Terminal.Verbosity.VERBOSE, "\tSerial: " + caInfo.certAndKey.cert.getSerialNumber());
            terminal.println(Terminal.Verbosity.VERBOSE, "\tExpiry: " + caInfo.certAndKey.cert.getNotAfter());
            terminal.println(Terminal.Verbosity.VERBOSE, "\tSignature Algorithm: " + caInfo.certAndKey.cert.getSigAlgName());

            validity = getCertificateValidityPeriod(terminal);
        }

        final boolean multipleCertificates = askMultipleCertificates(terminal);
        final List<CertOptions> certificates = new ArrayList<>();

        String nodeDescription = multipleCertificates ? "node #1" : "your nodes";
        while (true) {
            final CertOptions cert = getCertificateConfiguration(terminal, multipleCertificates, nodeDescription, validity, csr);
            terminal.println(Terminal.Verbosity.VERBOSE, "Generating the following " + (csr ? "CSR" : "Certificate") + ":");
            terminal.println(Terminal.Verbosity.VERBOSE, "\tName: " + cert.name);
            terminal.println(Terminal.Verbosity.VERBOSE, "\tSubject: " + cert.subject);
            terminal.println(Terminal.Verbosity.VERBOSE, "\tDNS Names: " + Strings.collectionToCommaDelimitedString(cert.dnsNames));
            terminal.println(Terminal.Verbosity.VERBOSE, "\tIP Names: " + Strings.collectionToCommaDelimitedString(cert.ipNames));
            terminal.println(Terminal.Verbosity.VERBOSE, "\tKey Size: " + cert.keySize);
            terminal.println(Terminal.Verbosity.VERBOSE, "\tValidity: " + toString(cert.validity));
            certificates.add(cert);

            if (multipleCertificates && terminal.promptYesNo("Generate additional certificates?", true)) {
                nodeDescription = "node #" + (certificates.size() + 1);
            } else {
                break;
            }
        }

        printHeader("What password do you want for your private key(s)?", terminal);
        char[] password;
        if (csr) {
            terminal.println("Your private key(s) will be stored as a PEM formatted file.");
            terminal.println("We recommend that you protect your private keys with a password");
            terminal.println("");
            terminal.println("If you do not wish to use a password, simply press <enter> at the prompt below.");
            password = readPassword(terminal, "Provide a password for the private key: ", true);
        } else {
            terminal.println("Your private key(s) will be stored in a PKCS#12 keystore file named \"http.p12\".");
            terminal.println("This type of keystore is always password protected, but it is possible to use a");
            terminal.println("blank password.");
            terminal.println("");
            terminal.println("If you wish to use a blank password, simply press <enter> at the prompt below.");
            password = readPassword(terminal, "Provide a password for the \"http.p12\" file: ", true);
        }

        printHeader("Where should we save the generated files?", terminal);
        if (csr) {
            terminal.println("A number of files will be generated including your private key(s),");
            terminal.println("certificate request(s), and sample configuration options for Elastic Stack products.");
        } else {
            terminal.println("A number of files will be generated including your private key(s),");
            terminal.println("public certificate(s), and sample configuration options for Elastic Stack products.");
        }
        terminal.println("");
        terminal.println("These files will be included in a single zip archive.");
        terminal.println("");
        Path output = resolvePath("elasticsearch-ssl-http.zip");
        output = tryReadInput(terminal, "What filename should be used for the output zip file?", output, this::resolvePath);

        writeZip(output, password, caInfo, certificates, env);
        terminal.println("");
        terminal.println("Zip file written to " + output);
    }

    /**
     * Resolve a filename as a Path (suppressing forbidden APIs).
     * Protected so tests can map String path-names to real path objects
     */
    @SuppressForbidden(reason = "CLI tool resolves files against working directory")
    protected Path resolvePath(String name) {
        return PathUtils.get(name).normalize().toAbsolutePath();
    }

    private void writeZip(Path file, char[] password, CertificateTool.CAInfo caInfo, List<CertOptions> certificates,
                          Environment env) throws UserException {
        if (Files.exists(file)) {
            throw new UserException(ExitCodes.IO_ERROR, "Output file '" + file + "' already exists");
        }

        boolean success = false;
        try {
            try (OutputStream fileStream = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW);
                 ZipOutputStream zipStream = new ZipOutputStream(fileStream, StandardCharsets.UTF_8)) {

                createZipDirectory(zipStream, "elasticsearch");
                if (certificates.size() == 1) {
                    writeCertificateAndKeyDetails(zipStream, "elasticsearch", certificates.get(0), caInfo, password, env);
                } else {
                    for (CertOptions cert : certificates) {
                        final String dirName = "elasticsearch/" + cert.name;
                        createZipDirectory(zipStream, dirName);
                        writeCertificateAndKeyDetails(zipStream, dirName, cert, caInfo, password, env);
                    }
                }

                if (caInfo != null && caInfo.generated) {
                    createZipDirectory(zipStream, "ca");
                    writeCertificateAuthority(zipStream, "ca", caInfo, env);
                }

                createZipDirectory(zipStream, "kibana");
                writeKibanaInfo(zipStream, "kibana", caInfo, env);

                /* TODO
                createZipDirectory(zipStream, "beats");
                writeBeatsInfo(zipStream, "beats", caInfo);

                createZipDirectory(zipStream, "logstash");
                writeLogstashInfo(zipStream, "logstash", caInfo);

                createZipDirectory(zipStream, "lang-clients");
                writeLangClientInfo(zipStream, "lang-clients", caInfo);

                createZipDirectory(zipStream, "other");
                writeMiscellaneousInfo(zipStream, "other", caInfo);
                */

                // set permissions to 600
                PosixFileAttributeView view = Files.getFileAttributeView(file, PosixFileAttributeView.class);
                if (view != null) {
                    view.setPermissions(Sets.newHashSet(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
                }

                success = true;
            } finally {
                if (success == false) {
                    Files.deleteIfExists(file);
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to write ZIP file '" + file + "'", e);
        }
    }

    private void createZipDirectory(ZipOutputStream zip, String name) throws IOException {
        ZipEntry entry = new ZipEntry(name + "/");
        assert entry.isDirectory();
        zip.putNextEntry(entry);
    }

    private void writeCertificateAndKeyDetails(ZipOutputStream zip, String dirName, CertOptions cert, CertificateTool.CAInfo ca,
                                               char[] password, Environment env) {
        // TODO : Should we add support for configuring PKI in ES?
        try {
            final KeyPair keyPair = CertGenUtils.generateKeyPair(cert.keySize);
            final GeneralNames sanList = CertificateTool.getSubjectAlternativeNamesValue(cert.ipNames, cert.dnsNames, List.of());
            final boolean hasPassword = password != null && password.length > 0;
            // TODO Add info to the READMEs so that the user could regenerate these certs if needed.
            // (i.e. show them the certutil cert command that they would need).
            if (ca == null) {
                // No local CA, generate a CSR instead
                final PKCS10CertificationRequest csr = CertGenUtils.generateCSR(keyPair, cert.subject, sanList);
                final String csrFile = "http-" + cert.name + ".csr";
                final String keyFile = "http-" + cert.name + ".key";
                final String certName = "http-" + cert.name + ".crt";
                final String ymlFile = "sample-elasticsearch.yml";
                final Map<String, String> substitutions = buildSubstitutions(env, Map.ofEntries(
                    Map.entry("CSR", csrFile),
                    Map.entry("KEY", keyFile),
                    Map.entry("CERT", certName),
                    Map.entry("YML", ymlFile),
                    Map.entry("PASSWORD", hasPassword ? "*" : "")));
                writeTextFile(zip, dirName + "/README.txt", ES_README_CSR, substitutions);
                writePemEntry(zip, dirName + "/" + csrFile, new JcaMiscPEMGenerator(csr));
                writePemEntry(zip, dirName + "/" + keyFile, generator(keyPair.getPrivate(), password));
                writeTextFile(zip, dirName + "/" + ymlFile, ES_YML_CSR, substitutions);
            } else {
                final ZonedDateTime notBefore = ZonedDateTime.now(ZoneOffset.UTC);
                final ZonedDateTime notAfter = notBefore.plus(cert.validity);
                Certificate certificate = CertGenUtils.generateSignedCertificate(cert.subject, sanList, keyPair, ca.certAndKey.cert,
                    ca.certAndKey.key, false, notBefore, notAfter, null);

                final String p12Name = "http.p12";
                final String ymlFile = "sample-elasticsearch.yml";
                final Map<String, String> substitutions = buildSubstitutions(env, Map.ofEntries(
                    Map.entry("P12", p12Name),
                    Map.entry("YML", ymlFile),
                    Map.entry("PASSWORD", hasPassword ? "*" : "")));
                writeTextFile(zip, dirName + "/README.txt", ES_README_P12, substitutions);
                writeKeyStore(zip, dirName + "/" + p12Name, certificate, keyPair.getPrivate(), password, ca.certAndKey.cert);
                writeTextFile(zip, dirName + "/" + ymlFile, ES_YML_P12, substitutions);
            }
        } catch (OperatorException | IOException | GeneralSecurityException e) {
            throw new ElasticsearchException("Failed to write certificate to ZIP file", e);
        }
    }

    private void writeCertificateAuthority(ZipOutputStream zip, String dirName, CertificateTool.CAInfo ca, Environment env) {
        assert ca != null;
        assert ca.generated;

        try {
            writeTextFile(zip, dirName + "/README.txt", CA_README_P12,
                buildSubstitutions(env, Map.of(
                    "P12", "ca.p12",
                    "DN", ca.certAndKey.cert.getSubjectX500Principal().getName(),
                    "PASSWORD", ca.password == null || ca.password.length == 0 ? "" : "*"
                )));
            final KeyStore pkcs12 = KeyStore.getInstance("PKCS12");
            pkcs12.load(null);
            pkcs12.setKeyEntry("ca", ca.certAndKey.key, ca.password, new Certificate[] { ca.certAndKey.cert });
            try (ZipEntryStream entry = new ZipEntryStream(zip, dirName + "/ca.p12")) {
                pkcs12.store(entry, ca.password);
            }
        } catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException e) {
            throw new ElasticsearchException("Failed to write CA to ZIP file", e);
        }
    }

    private void writeKibanaInfo(ZipOutputStream zip, String dirName, CertificateTool.CAInfo ca, Environment env) {
        final String caCertName = "elasticsearch-ca.pem";
        final String caCert = ca == null ? "" : caCertName;
        final String ymlFile = "sample-kibana.yml";

        final Map<String, String> substitutions = buildSubstitutions(env, Map.ofEntries(
            Map.entry("CA_CERT_NAME", caCertName),
            Map.entry("CA_CERT", caCert),
            Map.entry("YML", ymlFile)
        ));

        // TODO : Should we add support for client certs from Kibana to ES?

        try {
            writeTextFile(zip, dirName + "/README.txt", KIBANA_README, substitutions);
            if (ca != null) {
                writePemEntry(zip, dirName + "/" + caCert, new JcaMiscPEMGenerator(ca.certAndKey.cert));
            }
            writeTextFile(zip, dirName + "/" + ymlFile, KIBANA_YML, substitutions);
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to write Kibana details ZIP file", e);
        }
    }

    /**
     * Loads {@code resource} from the classpath, performs variable substitution on it, and then writes it to {@code writer}.
     */
    private void writeTextFile(ZipOutputStream zip, String outputName, String resource, Map<String, String> substitutions) {
        try (InputStream stream = getClass().getResourceAsStream("certutil-http/" + resource);
             ZipEntryStream entry = new ZipEntryStream(zip, outputName);
             PrintWriter writer = new PrintWriter(entry, false, StandardCharsets.UTF_8)) {
            if (stream == null) {
                throw new IllegalStateException("Cannot find internal resource " + resource);
            }
            copyWithSubstitutions(stream, writer, substitutions);
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot add resource " + resource + " to zip file", e);
        }
    }

    /**
     * Copies the input stream to the writer, while performing variable substitutions.
     * The variable substitution processing supports 2 constructs
     * <ol>
     * <li>
     * For each key in @{code substitutions}, any sequence of <code>${key}</code> in the input is replaced with the
     * substitution value.
     * </li>
     * <li>
     * Any line in the input that has the form <code>#if KEY</code> causes the following block to be output
     * if-only-if KEY exists with a non-empty value in {@code substitutions}.
     * A block is terminated with {@code #endif}. Lines with {@code #else} are also supported. Nested blocks are not supported.
     * </li>
     * </ol>
     */
    static void copyWithSubstitutions(InputStream stream, PrintWriter writer, Map<String, String> substitutions) throws IOException {
        boolean skip = false;
        for (String line : Streams.readAllLines(stream)) {
            for (Map.Entry<String, String> subst : substitutions.entrySet()) {
                line = line.replace("${" + subst.getKey() + "}", subst.getValue());
            }
            if (line.startsWith("#if ")) {
                final String key = line.substring(4).trim();
                skip = Strings.isNullOrEmpty(substitutions.get(key));
                continue;
            } else if (line.equals("#else")) {
                skip = skip == false;
                continue;
            } else if (line.equals("#endif")) {
                skip = false;
                continue;
            } else if (skip) {
                continue;
            }
            writer.println(line);
        }
    }

    private Map<String, String> buildSubstitutions(Environment env, Map<String, String> entries) {
        final Map<String, String> map = new HashMap<>(entries.size() + 4);
        ZonedDateTime now = ZonedDateTime.now().withNano(0);
        map.put("DATE", now.format(DateTimeFormatter.ISO_LOCAL_DATE));
        map.put("TIME", now.format(DateTimeFormatter.ISO_OFFSET_TIME));
        map.put("VERSION", Version.CURRENT.toString());
        map.put("CONF_DIR", env.configFile().toAbsolutePath().toString());
        map.putAll(entries);
        return map;
    }

    private void writeKeyStore(ZipOutputStream zip, String name, Certificate certificate, PrivateKey key, char[] password,
                               X509Certificate caCert) throws IOException, GeneralSecurityException {
        final KeyStore pkcs12 = KeyStore.getInstance("PKCS12");
        pkcs12.load(null);
        pkcs12.setKeyEntry("http", key, password, new Certificate[] { certificate });
        if (caCert != null) {
            pkcs12.setCertificateEntry("ca", caCert);
        }
        try (ZipEntryStream entry = new ZipEntryStream(zip, name)) {
            pkcs12.store(entry, password);
        }
    }

    private void writePemEntry(ZipOutputStream zip, String name, PemObjectGenerator generator) throws IOException {
        try (ZipEntryStream entry = new ZipEntryStream(zip, name);
             JcaPEMWriter pem = new JcaPEMWriter(new OutputStreamWriter(entry, StandardCharsets.UTF_8))) {
            pem.writeObject(generator);
            pem.flush();
        }
    }

    private JcaMiscPEMGenerator generator(PrivateKey privateKey, char[] password) throws IOException {
        if (password == null || password.length == 0) {
            return new JcaMiscPEMGenerator(privateKey);
        }
        return new JcaMiscPEMGenerator(privateKey, CertificateTool.getEncrypter(password));
    }

    private Period getCertificateValidityPeriod(Terminal terminal) {
        printHeader("How long should your certificates be valid?", terminal);
        terminal.println("Every certificate has an expiry date. When the expiry date is reached clients");
        terminal.println("will stop trusting your certificate and TLS connections will fail.");
        terminal.println("");
        terminal.println("Best practice suggests that you should either:");
        terminal.println("(a) set this to a short duration (90 - 120 days) and have automatic processes");
        terminal.println("to generate a new certificate before the old one expires, or");
        terminal.println("(b) set it to a longer duration (3 - 5 years) and then perform a manual update");
        terminal.println("a few months before it expires.");
        terminal.println("");
        terminal.println("You may enter the validity period in years (e.g. 3Y), months (e.g. 18M), or days (e.g. 90D)");
        terminal.println("");

        return readPeriodInput(terminal, "For how long should your certificate be valid?", DEFAULT_CERT_VALIDITY, 60);
    }

    private boolean askMultipleCertificates(Terminal terminal) {
        printHeader("Do you wish to generate one certificate per node?", terminal);
        terminal.println("If you have multiple nodes in your cluster, then you may choose to generate a");
        terminal.println("separate certificate for each of these nodes. Each certificate will have its");
        terminal.println("own private key, and will be issued for a specific hostname or IP address.");
        terminal.println("");
        terminal.println("Alternatively, you may wish to generate a single certificate that is valid");
        terminal.println("across all the hostnames or addresses in your cluster.");
        terminal.println("");
        terminal.println("If all of your nodes will be accessed through a single domain");
        terminal.println("(e.g. node01.es.example.com, node02.es.example.com, etc) then you may find it");
        terminal.println("simpler to generate one certificate with a wildcard hostname (*.es.example.com)");
        terminal.println("and use that across all of your nodes.");
        terminal.println("");
        terminal.println("However, if you do not have a common domain name, and you expect to add");
        terminal.println("additional nodes to your cluster in the future, then you should generate a");
        terminal.println("certificate per node so that you can more easily generate new certificates when");
        terminal.println("you provision new nodes.");
        terminal.println("");
        return terminal.promptYesNo("Generate a certificate per node?", false);
    }

    private CertOptions getCertificateConfiguration(Terminal terminal, boolean multipleCertificates, String nodeDescription,
                                                    Period validity, boolean csr) {

        String certName = null;
        if (multipleCertificates) {
            printHeader("What is the name of " + nodeDescription + "?", terminal);
            terminal.println("This name will be used as part of the certificate file name, and as a");
            terminal.println("descriptive name within the certificate.");
            terminal.println("");
            terminal.println("You can use any descriptive name that you like, but we recommend using the name");
            terminal.println("of the Elasticsearch node.");
            terminal.println("");
            certName = terminal.readText(nodeDescription + " name: ");
            nodeDescription = certName;
        }

        printHeader("Which hostnames will be used to connect to " + nodeDescription + "?", terminal);
        terminal.println("These hostnames will be added as \"DNS\" names in the \"Subject Alternative Name\"");
        terminal.println("(SAN) field in your certificate.");
        terminal.println("");
        terminal.println("You should list every hostname and variant that people will use to connect to");
        terminal.println("your cluster over http.");
        terminal.println("Do not list IP addresses here, you will be asked to enter them later.");
        terminal.println("");
        terminal.println("If you wish to use a wildcard certificate (for example *.es.example.com) you");
        terminal.println("can enter that here.");

        final List<String> dnsNames = new ArrayList<>();
        while (true) {
            terminal.println("");
            terminal.println("Enter all the hostnames that you need, one per line." );
            terminal.println("When you are done, press <ENTER> once more to move on to the next step.");
            terminal.println("");

            dnsNames.addAll(readMultiLineInput(terminal, this::validateHostname));
            if (dnsNames.isEmpty()) {
                terminal.println(Terminal.Verbosity.SILENT, "You did not enter any hostnames.");
                terminal.println("Clients are likely to encounter TLS hostname verification errors if they");
                terminal.println("connect to your cluster using a DNS name.");
            } else {
                terminal.println(Terminal.Verbosity.SILENT, "You entered the following hostnames.");
                terminal.println(Terminal.Verbosity.SILENT, "");
                dnsNames.forEach(s -> terminal.println(Terminal.Verbosity.SILENT, " - " + s));
            }
            terminal.println("");
            if (terminal.promptYesNo("Is this correct", true)) {
                break;
            } else {
                dnsNames.clear();
            }
        }

        printHeader("Which IP addresses will be used to connect to " + nodeDescription + "?", terminal);
        terminal.println("If your clients will ever connect to your nodes by numeric IP address, then you");
        terminal.println("can list these as valid IP \"Subject Alternative Name\" (SAN) fields in your");
        terminal.println("certificate.");
        terminal.println("");
        terminal.println("If you do not have fixed IP addresses, or not wish to support direct IP access");
        terminal.println("to your cluster then you can just press <ENTER> to skip this step.");

        final List<String> ipNames = new ArrayList<>();
        while (true) {
            terminal.println("");
            terminal.println("Enter all the IP addresses that you need, one per line.");
            terminal.println("When you are done, press <ENTER> once more to move on to the next step.");
            terminal.println("");

            ipNames.addAll(readMultiLineInput(terminal, this::validateIpAddress));
            if (ipNames.isEmpty()) {
                terminal.println(Terminal.Verbosity.SILENT, "You did not enter any IP addresses.");
            } else {
                terminal.println(Terminal.Verbosity.SILENT, "You entered the following IP addresses.");
                terminal.println(Terminal.Verbosity.SILENT, "");
                ipNames.forEach(s -> terminal.println(Terminal.Verbosity.SILENT, " - " + s));
            }
            terminal.println("");
            if (terminal.promptYesNo("Is this correct", true)) {
                break;
            } else {
                ipNames.clear();
            }
        }

        printHeader("Other certificate options", terminal);
        terminal.println("The generated certificate will have the following additional configuration");
        terminal.println("values. These values have been selected based on a combination of the");
        terminal.println("information you have provided above and secure defaults. You should not need to");
        terminal.println("change these values unless you have specific requirements.");
        terminal.println("");

        if (certName == null) {
            certName = dnsNames.stream().filter(n -> n.indexOf('*') == -1).findFirst()
                .or(() -> dnsNames.stream().map(s -> s.replace("*.", "")).findFirst())
                .orElse("elasticsearch");
        }
        X500Principal dn = buildDistinguishedName(certName);
        int keySize = DEFAULT_CERT_KEY_SIZE;
        while (true) {
            terminal.println(Terminal.Verbosity.SILENT, "Key Name: " + certName);
            terminal.println(Terminal.Verbosity.SILENT, "Subject DN: " + dn);
            terminal.println(Terminal.Verbosity.SILENT, "Key Size: " + keySize);
            terminal.println(Terminal.Verbosity.SILENT, "");
            if (terminal.promptYesNo("Do you wish to change any of these options?", false) == false) {
                break;
            }

            printHeader("What should your key be named?", terminal);
            if (csr) {
                terminal.println("This will be included in the name of the files that are generated");
            } else {
                terminal.println("This will be the entry name in the PKCS#12 keystore that is generated");
            }
            terminal.println("It is helpful to have a meaningful name for this key");
            terminal.println("");
            certName = tryReadInput(terminal, "Key Name", certName, Function.identity());

            printHeader("What subject DN should be used for your certificate?", terminal);
            terminal.println("This will be visible to clients.");
            terminal.println("It is helpful to have a meaningful name for each certificate");
            terminal.println("");
            dn = tryReadInput(terminal, "Subject DN", dn, name -> {
                try {
                    if (name.contains("=")) {
                        return new X500Principal(name);
                    } else {
                        return new X500Principal("CN=" + name);
                    }
                } catch (IllegalArgumentException e) {
                    terminal.println(Terminal.Verbosity.SILENT, "'" + name + "' is not a valid DN (" + e.getMessage() + ")");
                    return null;
                }
            });

            printHeader("What key size should your certificate have?", terminal);
            terminal.println("The RSA private key for your certificate has a fixed 'key size' (in bits).");
            terminal.println("Larger key sizes are generally more secure, but are also slower.");
            terminal.println("");
            terminal.println("We recommend that you use one of 2048, 3072 or 4096 bits for your key.");

            keySize = readKeySize(terminal, keySize);
            terminal.println("");
        }

        return new CertOptions(certName, dn, dnsNames, ipNames, keySize, validity);
    }

    private String validateHostname(String name) {
        if (DERIA5String.isIA5String(name)) {
            return null;
        } else {
            return name + " is not a valid DNS name";
        }
    }

    private String validateIpAddress(String ip) {
        if (InetAddresses.isInetAddress(ip)) {
            return null;
        } else {
            return ip + " is not a valid IP address";
        }
    }

    private X500Principal buildDistinguishedName(String name) {
        return new X500Principal("CN=" + name.replace(".", ",DC="));
    }

    private List<String> readMultiLineInput(Terminal terminal, Function<String, String> validator) {
        final List<String> lines = new ArrayList<>();
        while (true) {
            String input = terminal.readText("");
            if (Strings.isEmpty(input)) {
                break;
            } else {
                final String error = validator.apply(input);
                if (error == null) {
                    lines.add(input);
                } else {
                    terminal.println("Error: " + error);
                }
            }
        }
        return lines;
    }


    private boolean askCertSigningRequest(Terminal terminal) {
        printHeader("Do you wish to generate a Certificate Signing Request (CSR)?", terminal);

        terminal.println("A CSR is used when you want your certificate to be created by an existing");
        terminal.println("Certificate Authority (CA) that you do not control (that is, you don't have");
        terminal.println("access to the keys for that CA). ");
        terminal.println("");
        terminal.println("If you are in a corporate environment with a central security team, then you");
        terminal.println("may have an existing Corporate CA that can generate your certificate for you.");
        terminal.println("Infrastructure within your organisation may already be configured to trust this");
        terminal.println("CA, so it may be easier for clients to connect to Elasticsearch if you use a");
        terminal.println("CSR and send that request to the team that controls your CA.");
        terminal.println("");
        terminal.println("If you choose not to generate a CSR, this tool will generate a new certificate");
        terminal.println("for you. That certificate will be signed by a CA under your control. This is a");
        terminal.println("quick and easy way to secure your cluster with TLS, but you will need to");
        terminal.println("configure all your clients to trust that custom CA.");

        terminal.println("");
        return terminal.promptYesNo("Generate a CSR?", false);
    }

    private CertificateTool.CAInfo findExistingCA(Terminal terminal, Environment env) throws UserException {
        printHeader("What is the path to your CA?", terminal);

        terminal.println("Please enter the full pathname to the Certificate Authority that you wish to");
        terminal.println("use for signing your new http certificate. This can be in PKCS#12 (.p12), JKS");
        terminal.println("(.jks) or PEM (.crt, .key, .pem) format.");

        final Path caPath = requestPath("CA Path: ", terminal, env, true);
        final FileType fileType = guessFileType(caPath, terminal);
        switch (fileType) {

            case PKCS12:
            case JKS:
                terminal.println(Terminal.Verbosity.VERBOSE, "CA file " + caPath + " appears to be a " + fileType + " keystore");
                return readKeystoreCA(caPath, fileType, terminal);

            case PEM_KEY:
                printHeader("What is the path to your CA certificate?", terminal);
                terminal.println(caPath + " appears to be a PEM formatted private key file.");
                terminal.println("In order to use it for signing we also need access to the certificate");
                terminal.println("that corresponds to that key.");
                terminal.println("");
                final Path caCertPath = requestPath("CA Certificate: ", terminal, env, true);
                return readPemCA(caCertPath, caPath, terminal);

            case PEM_CERT:
                printHeader("What is the path to your CA key?", terminal);
                terminal.println(caPath + " appears to be a PEM formatted certificate file.");
                terminal.println("In order to use it for signing we also need access to the private key");
                terminal.println("that corresponds to that certificate.");
                terminal.println("");
                final Path caKeyPath = requestPath("CA Key: ", terminal, env, true);
                return readPemCA(caPath, caKeyPath, terminal);

            case PEM_CERT_CHAIN:
                terminal.println(Terminal.Verbosity.SILENT, "The file at " + caPath + " contains multiple certificates.");
                terminal.println("That type of file typically represents a certificate-chain");
                terminal.println("This tool requires a single certificate for the CA");
                throw new UserException(ExitCodes.DATA_ERROR, caPath + ": Unsupported file type (certificate chain)");


            case UNRECOGNIZED:
            default:
                terminal.println(Terminal.Verbosity.SILENT, "The file at " + caPath + " isn't a file type that this tool recognises.");
                terminal.println("Please try again with a CA in PKCS#12, JKS or PEM format");
                throw new UserException(ExitCodes.DATA_ERROR, caPath + ": Unrecognized file type");
        }
    }

    private CertificateTool.CAInfo createNewCA(Terminal terminal) {
        terminal.println("A new Certificate Authority will be generated for you");

        printHeader("CA Generation Options", terminal);
        terminal.println("The generated certificate authority will have the following configuration values.");
        terminal.println("These values have been selected based on secure defaults.");
        terminal.println("You should not need to change these values unless you have specific requirements.");
        terminal.println("");

        X500Principal dn = DEFAULT_CA_NAME;
        Period validity = DEFAULT_CA_VALIDITY;
        int keySize = DEFAULT_CA_KEY_SIZE;
        while (true) {
            terminal.println(Terminal.Verbosity.SILENT, "Subject DN: " + dn);
            terminal.println(Terminal.Verbosity.SILENT, "Validity: " + toString(validity));
            terminal.println(Terminal.Verbosity.SILENT, "Key Size: " + keySize);
            terminal.println(Terminal.Verbosity.SILENT, "");
            if (terminal.promptYesNo("Do you wish to change any of these options?", false) == false) {
                break;
            }

            printHeader("What should your CA be named?", terminal);
            terminal.println("Every client that connects to your Elasticsearch cluster will need to trust");
            terminal.println("this custom Certificate Authority.");
            terminal.println("It is helpful to have a meaningful name for this CA");
            terminal.println("");
            dn = tryReadInput(terminal, "CA Name", dn, name -> {
                try {
                    if (name.contains("=")) {
                        return new X500Principal(name);
                    } else {
                        return new X500Principal("CN=" + name);
                    }
                } catch (IllegalArgumentException e) {
                    terminal.println(Terminal.Verbosity.SILENT, "'" + name + "' is not a valid CA name (" + e.getMessage() + ")");
                    return null;
                }
            });

            printHeader("How long should your CA be valid?", terminal);
            terminal.println("Every certificate has an expiry date. When the expiry date is reached, clients");
            terminal.println("will stop trusting your Certificate Authority and TLS connections will fail.");
            terminal.println("");
            terminal.println("We recommend that you set this to a long duration (3 - 5 years) and then perform a");
            terminal.println("manual update a few months before it expires.");
            terminal.println("You may enter the validity period in years (e.g. 3Y), months (e.g. 18M), or days (e.g. 90D)");

            validity = readPeriodInput(terminal, "CA Validity", validity, 90);

            printHeader("What key size should your CA have?", terminal);
            terminal.println("The RSA private key for your Certificate Authority has a fixed 'key size' (in bits).");
            terminal.println("Larger key sizes are generally more secure, but are also slower.");
            terminal.println("");
            terminal.println("We recommend that you use one of 2048, 3072 or 4096 bits for your key.");

            keySize = readKeySize(terminal, keySize);
            terminal.println("");
        }

        try {
            final KeyPair keyPair = CertGenUtils.generateKeyPair(keySize);
            final ZonedDateTime notBefore = ZonedDateTime.now(ZoneOffset.UTC);
            final ZonedDateTime notAfter = notBefore.plus(validity);
            X509Certificate caCert = generateSignedCertificate(dn, null, keyPair, null, null, true, notBefore, notAfter, null);

            printHeader("CA password", terminal);
            terminal.println("We recommend that you protect your CA private key with a strong password.");
            terminal.println("If your key does not have a password (or the password can be easily guessed)");
            terminal.println("then anyone who gets a copy of the key file will be able to generate new certificates");
            terminal.println("and impersonate your Elasticsearch cluster.");
            terminal.println("");
            terminal.println("IT IS IMPORTANT THAT YOU REMEMBER THIS PASSWORD AND KEEP IT SECURE");
            terminal.println("");
            final char[] password = readPassword(terminal, "CA password: ", true);
            return new CertificateTool.CAInfo(caCert, keyPair.getPrivate(), true, password);
        } catch (GeneralSecurityException | CertIOException | OperatorCreationException e) {
            throw new IllegalArgumentException("Cannot generate CA key pair", e);
        }
    }

    /**
     * Read input from the terminal as a {@link Period}.
     * Package protected for testing purposes.
     */
    Period readPeriodInput(Terminal terminal, String prompt, Period defaultValue, int recommendedMinimumDays) {
        Period period = tryReadInput(terminal, prompt, defaultValue, input -> {
            String periodInput = input.replaceAll("[,\\s]", "");
            if (input.charAt(0) != 'P') {
                periodInput = "P" + periodInput;
            }
            try {
                final Period parsed = Period.parse(periodInput);
                final long approxDays = 30 * parsed.toTotalMonths() + parsed.getDays();
                if (approxDays < recommendedMinimumDays) {
                    terminal.println("The period '" + toString(parsed) + "' is less than the recommended period");
                    if (terminal.promptYesNo("Are you sure?", false) == false) {
                        return null;
                    }
                }
                return parsed;
            } catch (DateTimeParseException e) {
                terminal.println("Sorry, I do not understand '" + input + "' (" + e.getMessage() + ")");
                return null;
            }
        });
        return period;
    }

    private Integer readKeySize(Terminal terminal, int keySize) {
        return tryReadInput(terminal, "Key Size", keySize, input -> {
            try {
                final int size = Integer.parseInt(input);
                if (size < 1024) {
                    terminal.println("Keys must be at least 1024 bits");
                    return null;
                }
                if (size > 8192) {
                    terminal.println("Keys cannot be larger than 8192 bits");
                    return null;
                }
                if (size % 1024 != 0) {
                    terminal.println("The key size should be a multiple of 1024 bits");
                    return null;
                }
                return size;
            } catch (NumberFormatException e) {
                terminal.println("The key size must be a positive integer");
                return null;
            }
        });
    }

    private char[] readPassword(Terminal terminal, String prompt, boolean confirm) {
        while (true) {
            final char[] password = terminal.readSecret(prompt + " [<ENTER> for none]");
            if (password.length == 0) {
                return password;
            }
            if (CertificateTool.isAscii(password)) {
                if (confirm) {
                    final char[] again = terminal.readSecret("Repeat password to confirm: ");
                    if (Arrays.equals(password, again) == false) {
                        terminal.println("Passwords do not match");
                        continue;
                    }
                }
                if (CertificateTool.checkAndConfirmPasswordLengthForOpenSSLCompatibility(password, terminal, confirm) == false) {
                    continue;
                }
                return password;
            } else {
                terminal.println(Terminal.Verbosity.SILENT, "Passwords must be plain ASCII");
            }
        }
    }

    private CertificateTool.CAInfo readKeystoreCA(Path ksPath, FileType fileType, Terminal terminal) throws UserException {
        final String storeType = fileType == FileType.PKCS12 ? "PKCS12" : "jks";
        terminal.println("Reading a " + storeType + " keystore requires a password.");
        terminal.println("It is possible for the keystore's password to be blank,");
        terminal.println("in which case you can simply press <ENTER> at the prompt");
        final char[] password = terminal.readSecret("Password for " + ksPath.getFileName() + ":");
        try {
            final Map<Certificate, Key> keys = CertParsingUtils.readKeyPairsFromKeystore(ksPath, storeType, password, alias -> password);

            if (keys.size() != 1) {
                if (keys.isEmpty()) {
                    terminal.println(Terminal.Verbosity.SILENT, "The keystore at " + ksPath + " does not contain any keys ");
                } else {
                    terminal.println(Terminal.Verbosity.SILENT, "The keystore at " + ksPath + " contains " + keys.size() + " keys,");
                    terminal.println(Terminal.Verbosity.SILENT, "but this command requires a keystore with a single key");
                }
                terminal.println("Please try again with a keystore that contains exactly 1 private key entry");
                throw new UserException(ExitCodes.DATA_ERROR, "The CA keystore " + ksPath + " contains " + keys.size() + " keys");
            }
            final Map.Entry<Certificate, Key> pair = keys.entrySet().iterator().next();
            return new CertificateTool.CAInfo((X509Certificate) pair.getKey(), (PrivateKey) pair.getValue());
        } catch (IOException | GeneralSecurityException e) {
            throw new ElasticsearchException("Failed to read keystore " + ksPath, e);
        }
    }

    private CertificateTool.CAInfo readPemCA(Path certPath, Path keyPath, Terminal terminal) throws UserException {
        final X509Certificate cert = readCertificate(certPath, terminal);
        final PrivateKey key = readPrivateKey(keyPath, terminal);
        return new CertificateTool.CAInfo(cert, key);
    }

    private X509Certificate readCertificate(Path path, Terminal terminal) throws UserException {
        try {
            final X509Certificate[] certificates = CertParsingUtils.readX509Certificates(List.of(path));
            switch (certificates.length) {
                case 0:
                    terminal.errorPrintln("Could not read any certificates from " + path);
                    throw new UserException(ExitCodes.DATA_ERROR, path + ": No certificates found");
                case 1:
                    return certificates[0];
                default:
                    terminal.errorPrintln("Read [" + certificates.length + "] certificates from " + path + " but expected 1");
                    throw new UserException(ExitCodes.DATA_ERROR, path + ": Multiple certificates found");
            }
        } catch (CertificateException | IOException e) {
            throw new ElasticsearchException("Failed to read certificates from " + path, e);
        }
    }

    private PrivateKey readPrivateKey(Path path, Terminal terminal) {
        try {
            return PemUtils.readPrivateKey(path, () -> {
                terminal.println("");
                terminal.println("The PEM key stored in " + path + " requires a password.");
                terminal.println("");
                return terminal.readSecret("Password for " + path.getFileName() + ":");
            });
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to read private key from " + path, e);
        }
    }


    private boolean askExistingCertificateAuthority(Terminal terminal) {
        printHeader("Do you have an existing Certificate Authority (CA) key-pair that you wish to use to sign your certificate?", terminal);
        terminal.println("If you have an existing CA certificate and key, then you can use that CA to");
        terminal.println("sign your new http certificate. This allows you to use the same CA across");
        terminal.println("multiple Elasticsearch clusters which can make it easier to configure clients,");
        terminal.println("and may be easier for you to manage.");
        terminal.println("");
        terminal.println("If you do not have an existing CA, one will be generated for you.");
        terminal.println("");

        return terminal.promptYesNo("Use an existing CA?", false);
    }

    private <T> T tryReadInput(Terminal terminal, String prompt, T defaultValue, Function<String, T> parser) {
        final String defaultStr = defaultValue instanceof Period ? toString((Period) defaultValue) : String.valueOf(defaultValue);
        while (true) {
            final String input = terminal.readText(prompt + " [" + defaultStr + "] ");
            if (Strings.isEmpty(input)) {
                return defaultValue;
            }
            T parsed = parser.apply(input);
            if (parsed != null) {
                return parsed;
            }
        }
    }

    static String toString(Period period) {
        if (period == null) {
            return "N/A";
        }
        if (period.isZero()) {
            return "0d";
        }
        List<String> parts = new ArrayList<>(3);
        if (period.getYears() != 0) {
            parts.add(period.getYears() + "y");
        }
        if (period.getMonths() != 0) {
            parts.add(period.getMonths() + "m");
        }
        if (period.getDays() != 0) {
            parts.add(period.getDays() + "d");
        }
        return Strings.collectionToCommaDelimitedString(parts);
    }

    private Path requestPath(String prompt, Terminal terminal, Environment env, boolean requireExisting) {
        for (; ; ) {
            final String input = terminal.readText(prompt);
            final Path path = env.configFile().resolve(input).toAbsolutePath();

            if (path.getFileName() == null) {
                terminal.println(Terminal.Verbosity.SILENT, input + " is not a valid file");
                continue;
            }
            if (requireExisting == false || Files.isReadable(path)) {
                return path;
            }

            if (Files.notExists(path)) {
                terminal.println(Terminal.Verbosity.SILENT, "The file " + path + " does not exist");
            } else {
                terminal.println(Terminal.Verbosity.SILENT, "The file " + path + " cannot be read");
            }
        }
    }

    static FileType guessFileType(Path path, Terminal terminal) {
        // trust the extension for some file-types rather than inspecting the contents
        // we don't rely on filename for PEM files because
        // (a) users have a tendency to get things mixed up (e.g. naming something "key.crt")
        // (b) we need to distinguish between Certs & Keys, so a ".pem" file is ambiguous
        final String fileName = path == null ? "" : path.getFileName().toString().toLowerCase(Locale.ROOT);
        if (fileName.endsWith(".p12") || fileName.endsWith(".pfx") || fileName.endsWith(".pkcs12")) {
            return FileType.PKCS12;
        }
        if (fileName.endsWith(".jks")) {
            return FileType.JKS;
        }
        // Sniff the file. We could just try loading them, but then we need to catch a variety of exceptions
        // and guess what they mean. For example, loading a PKCS#12 needs a password, so we would need to
        // distinguish between a "wrong/missing password" exception and a "not a PKCS#12 file" exception.
        try (InputStream in = Files.newInputStream(path)) {
            byte[] leadingBytes = new byte[2];
            final int read = in.read(leadingBytes);
            if (read < leadingBytes.length) {
                // No supported file type has less than 2 bytes
                return FileType.UNRECOGNIZED;
            }
            if (Arrays.equals(leadingBytes, MAGIC_BYTES1_PKCS12) ||
                    Arrays.equals(leadingBytes, MAGIC_BYTES2_PKCS12) ||
                    Arrays.equals(leadingBytes, MAGIC_BYTES2_JDK16_PKCS12)) {
                return FileType.PKCS12;
            }
            if (Arrays.equals(leadingBytes, MAGIC_BYTES_JKS)) {
                return FileType.JKS;
            }
        } catch (IOException e) {
            terminal.errorPrintln("Failed to read from file " + path);
            terminal.errorPrintln(e.toString());
            return FileType.UNRECOGNIZED;
        }
        // Probably a PEM file, but we need to know what type of object(s) it holds
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            final List<FileType> types = lines.filter(s -> s.startsWith("-----BEGIN")).map(s -> {
                if (s.contains("BEGIN CERTIFICATE")) {
                    return FileType.PEM_CERT;
                } else if (s.contains("PRIVATE KEY")) {
                    return FileType.PEM_KEY;
                } else {
                    return null;
                }
            }).filter(ft -> ft != null).collect(Collectors.toList());
            switch (types.size()) {
                case 0:
                    // Not a PEM
                    return FileType.UNRECOGNIZED;
                case 1:
                    return types.get(0);
                default:
                    if (types.contains(FileType.PEM_KEY)) {
                        // A Key and something else. Could be a cert + key pair, but we don't support that
                        terminal.errorPrintln("Cannot determine a type for the PEM file " + path + " because it contains: ["
                            + Strings.collectionToCommaDelimitedString(types) + "]");
                    } else {
                        // Multiple certificates = chain
                        return FileType.PEM_CERT_CHAIN;
                    }
            }
        } catch (UncheckedIOException | IOException e) {
            terminal.errorPrintln("Cannot determine the file type for " + path);
            terminal.errorPrintln(e.toString());
            return FileType.UNRECOGNIZED;
        }
        return FileType.UNRECOGNIZED;
    }

    private void printHeader(String text, Terminal terminal) {
        terminal.println("");
        terminal.println(Terminal.Verbosity.SILENT, "## " + text);
        terminal.println("");
    }

    /**
     * The standard zip output stream cannot be wrapped safely in another stream, because its close method closes the
     * zip file, not just the entry.
     * This class handles close correctly for a single entry
     */
    private class ZipEntryStream extends OutputStream {

        private final ZipOutputStream zip;

        ZipEntryStream(ZipOutputStream zip, String name) throws IOException {
            this(zip, new ZipEntry(name));
        }

        ZipEntryStream(ZipOutputStream zip, ZipEntry entry) throws IOException {
            this.zip = zip;
            assert entry.isDirectory() == false;
            zip.putNextEntry(entry);
        }

        @Override
        public void write(int b) throws IOException {
            zip.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            zip.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            zip.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            zip.flush();
        }

        @Override
        public void close() throws IOException {
            zip.closeEntry();
        }
    }

    // For testing
    OptionParser getParser() {
        return parser;
    }
}
