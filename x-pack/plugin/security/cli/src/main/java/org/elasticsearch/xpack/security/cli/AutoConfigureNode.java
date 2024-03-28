/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.lucene.util.SetOnce;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.SettingsBasedSeedHostsProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.security.HttpResponse;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.security.auth.x500.X500Principal;

import static org.elasticsearch.common.ssl.PemUtils.parsePKCS8PemString;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.core.security.CommandLineHttpClient.createURL;

/**
 * Configures a new cluster node, by appending to the elasticsearch.yml, so that it forms a single node cluster with
 * Security enabled. Used to configure only the initial node of a cluster, and only before the first time that the node
 * is started. Subsequent nodes can be added to the cluster via the enrollment flow, but this is not used to
 * configure such nodes or to display the necessary configuration (ie the enrollment tokens) for such.
 *
 * This will NOT run if Security is explicitly configured or if the existing configuration otherwise clashes with the
 * intent of this (i.e. the node is configured so it might not form a single node cluster).
 */
public class AutoConfigureNode extends EnvironmentAwareCommand {

    public static final String AUTO_CONFIG_HTTP_ALT_DN = "CN=Elasticsearch security auto-configuration HTTP CA";
    public static final String AUTO_CONFIG_TRANSPORT_ALT_DN = "CN=Elasticsearch security auto-configuration transport CA";
    // the transport keystore is also used as a truststore
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final String TRANSPORT_AUTOGENERATED_KEYSTORE_NAME = "transport";
    private static final String TRANSPORT_KEY_KEYSTORE_ENTRY = "transport";
    private static final String TRANSPORT_CA_CERT_KEYSTORE_ENTRY = "transport_ca";
    private static final String ELASTICSEARCH_GROUP_OWNER = "elasticsearch";
    private static final int TRANSPORT_CERTIFICATE_DAYS = 99 * 365;
    private static final int TRANSPORT_CA_CERTIFICATE_DAYS = 99 * 365;
    private static final int TRANSPORT_KEY_SIZE = 4096;
    private static final int TRANSPORT_CA_KEY_SIZE = 4096;
    static final String HTTP_AUTOGENERATED_KEYSTORE_NAME = "http";
    static final String HTTP_KEY_KEYSTORE_ENTRY = "http";
    static final String HTTP_CA_KEY_KEYSTORE_ENTRY = "http_ca";
    static final String HTTP_AUTOGENERATED_CA_NAME = "http_ca";
    private static final int HTTP_CA_CERTIFICATE_DAYS = 3 * 365;
    private static final int HTTP_CA_KEY_SIZE = 4096;
    private static final int HTTP_CERTIFICATE_DAYS = 2 * 365;
    private static final int HTTP_KEY_SIZE = 4096;
    static final String TLS_GENERATED_CERTS_DIR_NAME = "certs";
    static final String AUTO_CONFIGURATION_START_MARKER =
        "#----------------------- BEGIN SECURITY AUTO CONFIGURATION -----------------------";
    static final String AUTO_CONFIGURATION_END_MARKER =
        "#----------------------- END SECURITY AUTO CONFIGURATION -------------------------";

    private final OptionSpec<String> enrollmentTokenParam = parser.accepts("enrollment-token", "The enrollment token to use")
        .withRequiredArg();
    private final boolean inReconfigureMode;
    private final BiFunction<Environment, String, CommandLineHttpClient> clientFunction;

    public AutoConfigureNode(boolean reconfigure, BiFunction<Environment, String, CommandLineHttpClient> clientFunction) {
        super("Generates all the necessary security configuration for a node in a secured cluster");
        // This "cli utility" is invoked from the node startup script, where it is passed all the
        // node startup options unfiltered. It cannot consume most of them, but it does need to inspect the `-E` ones.
        parser.allowsUnrecognizedOptions();
        this.inReconfigureMode = reconfigure;
        this.clientFunction = clientFunction;
    }

    public AutoConfigureNode(boolean reconfigure) {
        this(reconfigure, CommandLineHttpClient::new);
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        final boolean inEnrollmentMode = options.has(enrollmentTokenParam);

        // skipping security auto-configuration because node considered as restarting.
        for (Path dataPath : env.dataFiles()) {
            if (Files.isDirectory(dataPath) && false == isDirEmpty(dataPath)) {
                final String msg = "Skipping security auto configuration because it appears that the node is not starting up for the "
                    + "first time. The node might already be part of a cluster and this auto setup utility is designed to configure "
                    + "Security for new clusters only.";
                notifyOfFailure(inEnrollmentMode, terminal, Terminal.Verbosity.VERBOSE, ExitCodes.NOOP, msg);
            }
        }

        // pre-flight checks for the files that are going to be changed
        final Path ymlPath = env.configFile().resolve("elasticsearch.yml");
        // it is odd for the `elasticsearch.yml` file to be missing or not be a regular (the node won't start)
        // but auto configuration should not be concerned with fixing it (by creating the file) and let the node startup fail
        if (false == Files.exists(ymlPath) || false == Files.isRegularFile(ymlPath, LinkOption.NOFOLLOW_LINKS)) {
            final String msg = String.format(
                Locale.ROOT,
                "Skipping security auto configuration because the configuration file [%s] is missing or is not a regular file",
                ymlPath
            );
            notifyOfFailure(inEnrollmentMode, terminal, Terminal.Verbosity.NORMAL, ExitCodes.CONFIG, msg);
        }
        // If the node's yml configuration is not readable, most probably auto-configuration isn't run under the suitable user
        if (false == Files.isReadable(ymlPath)) {
            final String msg = String.format(
                Locale.ROOT,
                "Skipping security auto configuration because the current user does not have permission to read "
                    + " configuration file [%s]",
                ymlPath
            );
            notifyOfFailure(inEnrollmentMode, terminal, Terminal.Verbosity.NORMAL, ExitCodes.NOOP, msg);
        }
        final Path keystorePath = KeyStoreWrapper.keystorePath(env.configFile());
        // Inform that auto-configuration will not run if keystore cannot be read.
        if (Files.exists(keystorePath)
            && (false == Files.isRegularFile(keystorePath, LinkOption.NOFOLLOW_LINKS) || false == Files.isReadable(keystorePath))) {
            final String msg = String.format(
                Locale.ROOT,
                "Skipping security auto configuration because the node keystore file [%s] is not a readable regular file",
                keystorePath
            );
            notifyOfFailure(inEnrollmentMode, terminal, Terminal.Verbosity.NORMAL, ExitCodes.NOOP, msg);
        }

        if (inReconfigureMode) {
            if (false == inEnrollmentMode) {
                throw new UserException(ExitCodes.USAGE, "enrollment-token is a mandatory parameter when reconfiguring the node");
            }
            env = possiblyReconfigureNode(env, terminal, options, processInfo);
        }

        // only perform auto-configuration if the existing configuration is not conflicting (eg Security already enabled)
        // if it is, silently skip auto configuration
        checkExistingConfiguration(env.settings(), inEnrollmentMode, terminal);

        final ZonedDateTime autoConfigDate = ZonedDateTime.now(ZoneOffset.UTC);
        final Path tempGeneratedTlsCertsDir = env.configFile()
            .resolve(String.format(Locale.ROOT, TLS_GENERATED_CERTS_DIR_NAME + ".%d.tmp", autoConfigDate.toInstant().getEpochSecond()));
        try {
            // it is useful to pre-create the sub-config dir in order to check that the config dir is writable and that file owners match
            Files.createDirectory(tempGeneratedTlsCertsDir);
            // set permissions to 750, don't rely on umask, we assume auto configuration preserves ownership so we don't have to
            // grant "group" or "other" permissions
            PosixFileAttributeView view = Files.getFileAttributeView(tempGeneratedTlsCertsDir, PosixFileAttributeView.class);
            if (view != null) {
                view.setPermissions(PosixFilePermissions.fromString("rwxr-x---"));
            }
        } catch (Throwable t) {
            try {
                deleteDirectory(tempGeneratedTlsCertsDir);
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            // the config dir is probably read-only, either because this auto-configuration runs as a different user from the install user,
            // or if the admin explicitly makes configuration immutable (read-only), both of which are reasons to skip auto-configuration
            // this will show a message to the console (the first time the node starts) and auto-configuration is effectively bypassed
            // the message will not be subsequently shown (because auto-configuration doesn't run for node restarts)
            throw new UserException(ExitCodes.CANT_CREATE, "Could not create auto-configuration directory", t);
        }

        // Check that the created auto-config dir has the same owner as the config dir.
        // This is a sort of sanity check.
        // If the node process works OK given the owner of the config dir, it should also tolerate the auto-created config dir,
        // provided that they both have the same owner and permissions.
        final UserPrincipal newFileOwner = Files.getOwner(tempGeneratedTlsCertsDir, LinkOption.NOFOLLOW_LINKS);
        if (false == newFileOwner.equals(Files.getOwner(env.configFile(), LinkOption.NOFOLLOW_LINKS))) {
            // the following is only printed once, if the node starts successfully
            UserException userException = new UserException(
                ExitCodes.CONFIG,
                "Aborting auto configuration because of config dir ownership mismatch. Config dir is owned by "
                    + Files.getOwner(env.configFile(), LinkOption.NOFOLLOW_LINKS).getName()
                    + " but auto-configuration directory would be owned by "
                    + newFileOwner.getName()
            );
            try {
                deleteDirectory(tempGeneratedTlsCertsDir);
            } catch (Exception ex) {
                userException.addSuppressed(ex);
            }
            throw userException;
        }
        final X509Certificate transportCaCert;
        final PrivateKey transportKey;
        final X509Certificate transportCert;
        final PrivateKey httpCaKey;
        final X509Certificate httpCaCert;
        final PrivateKey httpKey;
        final X509Certificate httpCert;
        final List<String> transportAddresses;
        final String cnValue = NODE_NAME_SETTING.exists(env.settings()) ? NODE_NAME_SETTING.get(env.settings()) : System.getenv("HOSTNAME");
        final X500Principal certificatePrincipal = new X500Principal("CN=" + cnValue);
        final X500Principal httpCaPrincipal = new X500Principal(AUTO_CONFIG_HTTP_ALT_DN);
        final X500Principal transportCaPrincipal = new X500Principal(AUTO_CONFIG_TRANSPORT_ALT_DN);

        if (inEnrollmentMode) {
            // this is an enrolling node, get HTTP CA key/certificate and transport layer key/certificate from another node
            final EnrollmentToken enrollmentToken;
            try {
                enrollmentToken = EnrollmentToken.decodeFromString(enrollmentTokenParam.value(options));
            } catch (Exception e) {
                try {
                    deleteDirectory(tempGeneratedTlsCertsDir);
                } catch (Exception ex) {
                    e.addSuppressed(ex);
                }
                terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
                terminal.errorPrintln(
                    Terminal.Verbosity.VERBOSE,
                    "Failed to parse enrollment token : " + enrollmentTokenParam.value(options) + " . Error was: " + e.getMessage()
                );
                terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
                terminal.errorPrintln(Terminal.Verbosity.VERBOSE, ExceptionsHelper.stackTrace(e));
                terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
                throw new UserException(ExitCodes.DATA_ERROR, "Aborting auto configuration. Invalid enrollment token", e);
            }

            final CommandLineHttpClient client = clientFunction.apply(env, enrollmentToken.getFingerprint());

            // We don't wait for cluster health here. If the user has a token, it means that at least the first node has started
            // successfully so we expect the cluster to be healthy already. If not, this is a sign of a problem and we should bail.
            HttpResponse enrollResponse = null;
            URL enrollNodeUrl = null;
            for (String address : enrollmentToken.getBoundAddress()) {
                try {
                    enrollNodeUrl = createURL(new URL("https://" + address), "/_security/enroll/node", "");
                    enrollResponse = client.execute(
                        "GET",
                        enrollNodeUrl,
                        new SecureString(enrollmentToken.getApiKey().toCharArray()),
                        () -> null,
                        CommandLineHttpClient::responseBuilder
                    );
                    break;
                } catch (Exception e) {
                    terminal.errorPrint(
                        Terminal.Verbosity.NORMAL,
                        "Unable to communicate with the node on " + enrollNodeUrl + ". Error was " + e.getMessage()
                    );
                }
            }
            if (enrollResponse == null || enrollResponse.getHttpStatus() != 200) {
                UserException userException = new UserException(
                    ExitCodes.UNAVAILABLE,
                    "Aborting enrolling to cluster. "
                        + "Could not communicate with the node on any of the addresses from the enrollment token. All of "
                        + enrollmentToken.getBoundAddress()
                        + " were attempted."
                );
                try {
                    deleteDirectory(tempGeneratedTlsCertsDir);
                } catch (Exception ex) {
                    userException.addSuppressed(ex);
                }
                throw userException;
            }
            final Map<String, Object> responseMap = enrollResponse.getResponseBody();
            if (responseMap == null) {
                UserException userException = new UserException(
                    ExitCodes.DATA_ERROR,
                    "Aborting enrolling to cluster. Empty response when calling the enroll node API (" + enrollNodeUrl + ")"
                );
                try {
                    deleteDirectory(tempGeneratedTlsCertsDir);
                } catch (Exception ex) {
                    userException.addSuppressed(ex);
                }
                throw userException;
            }
            final List<String> missingFields = new ArrayList<>();
            final String httpCaKeyPem = (String) responseMap.get("http_ca_key");
            if (Strings.isNullOrEmpty(httpCaKeyPem)) {
                missingFields.add("http_ca_key");
            }
            final String httpCaCertPem = (String) responseMap.get("http_ca_cert");
            if (Strings.isNullOrEmpty(httpCaCertPem)) {
                missingFields.add("http_ca_cert");
            }
            final String transportKeyPem = (String) responseMap.get("transport_key");
            if (Strings.isNullOrEmpty(transportKeyPem)) {
                missingFields.add("transport_key");
            }
            final String transportCaCertPem = (String) responseMap.get("transport_ca_cert");
            if (Strings.isNullOrEmpty(transportCaCertPem)) {
                missingFields.add("transport_ca_cert");
            }
            final String transportCertPem = (String) responseMap.get("transport_cert");
            if (Strings.isNullOrEmpty(transportCertPem)) {
                missingFields.add("transport_cert");
            }
            transportAddresses = getTransportAddresses(responseMap);
            if (null == transportAddresses || transportAddresses.isEmpty()) {
                missingFields.add("nodes_addresses");
            }
            if (false == missingFields.isEmpty()) {
                UserException userException = new UserException(
                    ExitCodes.DATA_ERROR,
                    "Aborting enrolling to cluster. Invalid response when calling the enroll node API ("
                        + enrollNodeUrl
                        + "). "
                        + "The following fields were empty or missing : "
                        + missingFields
                );
                try {
                    deleteDirectory(tempGeneratedTlsCertsDir);
                } catch (Exception ex) {
                    userException.addSuppressed(ex);
                }
                throw userException;
            }
            transportCaCert = parseCertificateFromPem(transportCaCertPem, terminal);
            httpCaKey = parseKeyFromPem(httpCaKeyPem, terminal);
            httpCaCert = parseCertificateFromPem(httpCaCertPem, terminal);
            transportKey = parseKeyFromPem(transportKeyPem, terminal);
            transportCert = parseCertificateFromPem(transportCertPem, terminal);
        } else {
            // this is the initial node, generate HTTP CA key/certificate and transport layer key/certificate ourselves
            try {
                transportAddresses = List.of();
                // self-signed CA for transport layer
                final KeyPair transportCaKeyPair = CertGenUtils.generateKeyPair(TRANSPORT_CA_KEY_SIZE);
                final PrivateKey transportCaKey = transportCaKeyPair.getPrivate();
                transportCaCert = CertGenUtils.generateSignedCertificate(
                    transportCaPrincipal,
                    null,
                    transportCaKeyPair,
                    null,
                    null,
                    true,
                    TRANSPORT_CA_CERTIFICATE_DAYS,
                    SIGNATURE_ALGORITHM
                );
                // transport key/certificate
                final KeyPair transportKeyPair = CertGenUtils.generateKeyPair(TRANSPORT_KEY_SIZE);
                transportKey = transportKeyPair.getPrivate();
                transportCert = CertGenUtils.generateSignedCertificate(
                    certificatePrincipal,
                    null, // transport only validates certificates not the hostname
                    transportKeyPair,
                    transportCaCert,
                    transportCaKey,
                    false,
                    TRANSPORT_CERTIFICATE_DAYS,
                    SIGNATURE_ALGORITHM
                );

                final KeyPair httpCaKeyPair = CertGenUtils.generateKeyPair(HTTP_CA_KEY_SIZE);
                httpCaKey = httpCaKeyPair.getPrivate();
                // self-signed CA
                httpCaCert = CertGenUtils.generateSignedCertificate(
                    httpCaPrincipal,
                    null,
                    httpCaKeyPair,
                    null,
                    null,
                    true,
                    HTTP_CA_CERTIFICATE_DAYS,
                    SIGNATURE_ALGORITHM
                );
            } catch (Throwable t) {
                try {
                    deleteDirectory(tempGeneratedTlsCertsDir);
                } catch (Exception ex) {
                    t.addSuppressed(ex);
                }
                // this is an error which mustn't be ignored during node startup
                // the exit code for unhandled Exceptions is "1"
                throw t;
            }
        }
        try {
            final KeyPair httpKeyPair = CertGenUtils.generateKeyPair(HTTP_KEY_SIZE);
            httpKey = httpKeyPair.getPrivate();
            // non-CA
            httpCert = CertGenUtils.generateSignedCertificate(
                certificatePrincipal,
                getSubjectAltNames(env.settings()),
                httpKeyPair,
                httpCaCert,
                httpCaKey,
                false,
                HTTP_CERTIFICATE_DAYS,
                SIGNATURE_ALGORITHM,
                Set.of(new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth))
            );

            // the HTTP CA PEM file is provided "just in case". The node doesn't use it, but clients (configured manually, outside of the
            // enrollment process) might indeed need it, and it is currently impossible to retrieve it
            fullyWriteFile(
                tempGeneratedTlsCertsDir,
                HTTP_AUTOGENERATED_CA_NAME + ".crt",
                false,
                inReconfigureMode ? ELASTICSEARCH_GROUP_OWNER : null,
                stream -> {
                    try (
                        JcaPEMWriter pemWriter = new JcaPEMWriter(
                            new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))
                        )
                    ) {
                        pemWriter.writeObject(httpCaCert);
                    }
                }
            );
        } catch (Throwable t) {
            try {
                deleteDirectory(tempGeneratedTlsCertsDir);
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            // this is an error which mustn't be ignored during node startup
            // the exit code for unhandled Exceptions is "1"
            throw t;
        }

        // save the existing keystore before replacing
        final Path keystoreBackupPath = env.configFile()
            .resolve(
                String.format(Locale.ROOT, KeyStoreWrapper.KEYSTORE_FILENAME + ".%d.orig", autoConfigDate.toInstant().getEpochSecond())
            );
        if (Files.exists(keystorePath)) {
            try {
                Files.copy(keystorePath, keystoreBackupPath, StandardCopyOption.COPY_ATTRIBUTES);
            } catch (Throwable t) {
                try {
                    deleteDirectory(tempGeneratedTlsCertsDir);
                } catch (Exception ex) {
                    t.addSuppressed(ex);
                }
                throw t;
            }
        }

        final SetOnce<SecureString> nodeKeystorePassword = new SetOnce<>();
        try (KeyStoreWrapper nodeKeystore = KeyStoreWrapper.bootstrap(env.configFile(), () -> {
            nodeKeystorePassword.set(new SecureString(terminal.readSecret("")));
            return nodeKeystorePassword.get().clone();
        })) {
            // do not overwrite keystore entries
            // instead expect the admin to manually remove them herself
            if (nodeKeystore.getSettingNames().contains("xpack.security.transport.ssl.keystore.secure_password")
                || nodeKeystore.getSettingNames().contains("xpack.security.transport.ssl.truststore.secure_password")
                || nodeKeystore.getSettingNames().contains("xpack.security.http.ssl.keystore.secure_password")) {
                // this error condition is akin to condition of existing configuration in the yml file
                // this is not a fresh install and the admin has something planned for Security
                // Even though this is probably invalid configuration, do NOT fix it, let the node fail to start in its usual way.
                // Still display a message, because this can be tricky to figure out (why auto-conf did not run) if by mistake.
                throw new UserException(
                    ExitCodes.CONFIG,
                    "Aborting auto configuration because the node keystore contains password settings already"
                );
            }
            try (SecureString transportKeystorePassword = newKeystorePassword()) {
                KeyStore transportKeystore = KeyStore.getInstance("PKCS12");
                transportKeystore.load(null);
                // the PKCS12 keystore and the contained private key use the same password
                transportKeystore.setKeyEntry(
                    TRANSPORT_KEY_KEYSTORE_ENTRY,
                    transportKey,
                    transportKeystorePassword.getChars(),
                    new Certificate[] { transportCert }
                );
                transportKeystore.setCertificateEntry(TRANSPORT_CA_CERT_KEYSTORE_ENTRY, transportCaCert);
                fullyWriteFile(
                    tempGeneratedTlsCertsDir,
                    TRANSPORT_AUTOGENERATED_KEYSTORE_NAME + ".p12",
                    false,
                    inReconfigureMode ? ELASTICSEARCH_GROUP_OWNER : null,
                    stream -> transportKeystore.store(stream, transportKeystorePassword.getChars())
                );
                nodeKeystore.setString("xpack.security.transport.ssl.keystore.secure_password", transportKeystorePassword.getChars());
                // we use the same PKCS12 file for the keystore and the truststore
                nodeKeystore.setString("xpack.security.transport.ssl.truststore.secure_password", transportKeystorePassword.getChars());
            }
            try (SecureString httpKeystorePassword = newKeystorePassword()) {
                KeyStore httpKeystore = KeyStore.getInstance("PKCS12");
                httpKeystore.load(null);
                // the keystore contains both the node's and the CA's private keys
                // both keys are encrypted using the same password as the PKCS12 keystore they're contained in
                httpKeystore.setKeyEntry(
                    HTTP_CA_KEY_KEYSTORE_ENTRY,
                    httpCaKey,
                    httpKeystorePassword.getChars(),
                    new Certificate[] { httpCaCert }
                );
                httpKeystore.setKeyEntry(
                    HTTP_KEY_KEYSTORE_ENTRY,
                    httpKey,
                    httpKeystorePassword.getChars(),
                    new Certificate[] { httpCert, httpCaCert }
                );
                fullyWriteFile(
                    tempGeneratedTlsCertsDir,
                    HTTP_AUTOGENERATED_KEYSTORE_NAME + ".p12",
                    false,
                    inReconfigureMode ? ELASTICSEARCH_GROUP_OWNER : null,
                    stream -> httpKeystore.store(stream, httpKeystorePassword.getChars())
                );
                nodeKeystore.setString("xpack.security.http.ssl.keystore.secure_password", httpKeystorePassword.getChars());
            }
            // finally overwrites the node keystore (if the keystores have been successfully written)
            nodeKeystore.save(env.configFile(), nodeKeystorePassword.get() == null ? new char[0] : nodeKeystorePassword.get().getChars());
        } catch (Throwable t) {
            // restore keystore to revert possible keystore bootstrap
            try {
                if (Files.exists(keystoreBackupPath)) {
                    Files.move(
                        keystoreBackupPath,
                        keystorePath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.COPY_ATTRIBUTES
                    );
                } else {
                    Files.deleteIfExists(keystorePath);
                }
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            try {
                deleteDirectory(tempGeneratedTlsCertsDir);
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            throw t;
        } finally {
            if (nodeKeystorePassword.get() != null) {
                nodeKeystorePassword.get().close();
            }
        }

        try {
            // all certs and keys have been generated in the temp certs dir, therefore:
            // 1. backup (move) any previously existing tls certs dir (this backup is NOT removed when auto-conf finishes)
            if (Files.exists(env.configFile().resolve(TLS_GENERATED_CERTS_DIR_NAME))) {
                moveDirectory(
                    env.configFile().resolve(TLS_GENERATED_CERTS_DIR_NAME),
                    env.configFile()
                        .resolve(
                            String.format(
                                Locale.ROOT,
                                TLS_GENERATED_CERTS_DIR_NAME + ".%d.orig",
                                autoConfigDate.toInstant().getEpochSecond()
                            )
                        )
                );
            }
            // 2. move the newly populated temp certs dir to its permanent static dir name
            moveDirectory(tempGeneratedTlsCertsDir, env.configFile().resolve(TLS_GENERATED_CERTS_DIR_NAME));
        } catch (Throwable t) {
            // restore keystore to revert possible keystore bootstrap
            try {
                if (Files.exists(keystoreBackupPath)) {
                    Files.move(
                        keystoreBackupPath,
                        keystorePath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.COPY_ATTRIBUTES
                    );
                } else {
                    Files.deleteIfExists(keystorePath);
                }
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            // revert any previously existing TLS certs
            try {
                if (Files.exists(
                    env.configFile()
                        .resolve(
                            String.format(
                                Locale.ROOT,
                                TLS_GENERATED_CERTS_DIR_NAME + ".%d.orig",
                                autoConfigDate.toInstant().getEpochSecond()
                            )
                        )
                )) {
                    moveDirectory(
                        env.configFile()
                            .resolve(
                                String.format(
                                    Locale.ROOT,
                                    TLS_GENERATED_CERTS_DIR_NAME + ".%d.orig",
                                    autoConfigDate.toInstant().getEpochSecond()
                                )
                            ),
                        env.configFile().resolve(TLS_GENERATED_CERTS_DIR_NAME)
                    );
                }
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            try {
                deleteDirectory(tempGeneratedTlsCertsDir);
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            throw t;
        }

        try {
            // final Environment to be used in the lambda below
            final Environment localFinalEnv = env;
            final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss", Locale.ROOT);
            List<String> existingConfigLines = Files.readAllLines(ymlPath, StandardCharsets.UTF_8);
            fullyWriteFile(env.configFile(), "elasticsearch.yml", true, stream -> {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))) {
                    // start with the existing config lines
                    for (String line : existingConfigLines) {
                        bw.write(line);
                        bw.newLine();
                    }
                    bw.newLine();
                    bw.write(AUTO_CONFIGURATION_START_MARKER);
                    bw.newLine();
                    bw.write("#");
                    bw.newLine();
                    bw.write("# The following settings, TLS certificates, and keys have been automatically      ");
                    bw.newLine();
                    bw.write("# generated to configure Elasticsearch security features on ");
                    bw.write(autoConfigDate.format(dateTimeFormatter));
                    bw.newLine();
                    bw.write("#");
                    // TODO add link to docs
                    bw.newLine();
                    bw.write("# --------------------------------------------------------------------------------");
                    bw.newLine();
                    bw.newLine();
                    bw.write("# Enable security features");
                    bw.newLine();
                    bw.write(XPackSettings.SECURITY_ENABLED.getKey() + ": true");
                    bw.newLine();
                    bw.newLine();
                    // Set enrollment mode to true unless user explicitly set it to false themselves
                    if (false == (localFinalEnv.settings().hasValue(XPackSettings.ENROLLMENT_ENABLED.getKey())
                        && false == XPackSettings.ENROLLMENT_ENABLED.get(localFinalEnv.settings()))) {
                        bw.write(XPackSettings.ENROLLMENT_ENABLED.getKey() + ": true");
                        bw.newLine();
                        bw.newLine();
                    }
                    bw.write("# Enable encryption for HTTP API client connections, such as Kibana, Logstash, and Agents");
                    bw.newLine();
                    bw.write("xpack.security.http.ssl:");
                    bw.newLine();
                    bw.write("  enabled: true");
                    bw.newLine();
                    bw.write("  keystore.path: " + TLS_GENERATED_CERTS_DIR_NAME + "/" + HTTP_AUTOGENERATED_KEYSTORE_NAME + ".p12");
                    bw.newLine();
                    bw.newLine();
                    bw.write("# Enable encryption and mutual authentication between cluster nodes");
                    bw.newLine();
                    bw.write("xpack.security.transport.ssl:");
                    bw.newLine();
                    bw.write("  enabled: true");
                    bw.newLine();
                    bw.write("  verification_mode: certificate");
                    bw.newLine();
                    bw.write("  keystore.path: " + TLS_GENERATED_CERTS_DIR_NAME + "/" + TRANSPORT_AUTOGENERATED_KEYSTORE_NAME + ".p12");
                    bw.newLine();
                    // we use the keystore as a truststore in order to minimize the number of auto-generated resources,
                    // and also because a single file is more idiomatic to the scheme of a shared secret between the cluster nodes
                    // no one should only need the TLS cert without the associated key for the transport layer
                    bw.write("  truststore.path: " + TLS_GENERATED_CERTS_DIR_NAME + "/" + TRANSPORT_AUTOGENERATED_KEYSTORE_NAME + ".p12");
                    if (inEnrollmentMode) {
                        bw.newLine();
                        bw.write("# Discover existing nodes in the cluster");
                        bw.newLine();
                        bw.write(
                            DISCOVERY_SEED_HOSTS_SETTING.getKey()
                                + ": ["
                                + transportAddresses.stream().map(p -> '"' + p + '"').collect(Collectors.joining(", "))
                                + "]"
                        );
                        bw.newLine();
                    } else {
                        // we have configured TLS on the transport layer with newly generated certs and keys,
                        // hence this node cannot form a multi-node cluster
                        // if we don't set the following the node might trip the discovery bootstrap check
                        if (false == DiscoveryModule.isSingleNodeDiscovery(localFinalEnv.settings())
                            && false == ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.exists(localFinalEnv.settings())) {
                            bw.newLine();
                            bw.write("# Create a new cluster with the current node only");
                            bw.newLine();
                            bw.write("# Additional nodes can still join the cluster later");
                            bw.newLine();
                            bw.write(
                                ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey()
                                    + ": "
                                    + initialMasterNodesSettingValue(localFinalEnv)
                            );
                            bw.newLine();
                        }
                    }

                    // if any address settings have been set, assume the admin has thought it through wrt to addresses,
                    // and don't try to be smart and mess with that
                    if (false == (localFinalEnv.settings().hasValue(HttpTransportSettings.SETTING_HTTP_HOST.getKey())
                        || localFinalEnv.settings().hasValue(HttpTransportSettings.SETTING_HTTP_BIND_HOST.getKey())
                        || localFinalEnv.settings().hasValue(HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST.getKey())
                        || localFinalEnv.settings().hasValue(NetworkService.GLOBAL_NETWORK_HOST_SETTING.getKey())
                        || localFinalEnv.settings().hasValue(NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.getKey())
                        || localFinalEnv.settings().hasValue(NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.getKey()))) {
                        bw.newLine();
                        bw.write("# Allow HTTP API connections from anywhere");
                        bw.newLine();
                        bw.write("# Connections are encrypted and require user authentication");
                        bw.newLine();
                        bw.write(HttpTransportSettings.SETTING_HTTP_HOST.getKey() + ": 0.0.0.0");
                        bw.newLine();
                    }
                    if (false == (localFinalEnv.settings().hasValue(TransportSettings.HOST.getKey())
                        || localFinalEnv.settings().hasValue(TransportSettings.BIND_HOST.getKey())
                        || localFinalEnv.settings().hasValue(TransportSettings.PUBLISH_HOST.getKey())
                        || localFinalEnv.settings().hasValue(NetworkService.GLOBAL_NETWORK_HOST_SETTING.getKey())
                        || localFinalEnv.settings().hasValue(NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.getKey())
                        || localFinalEnv.settings().hasValue(NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.getKey()))) {
                        bw.newLine();
                        bw.write("# Allow other nodes to join the cluster from anywhere");
                        bw.newLine();
                        bw.write("# Connections are encrypted and mutually authenticated");
                        bw.newLine();
                        if (false == inEnrollmentMode
                            || false == anyRemoteHostNodeAddress(transportAddresses, NetworkUtils.getAllAddresses())) {
                            bw.write("#");
                        }
                        bw.write(TransportSettings.HOST.getKey() + ": 0.0.0.0");
                        bw.newLine();
                    }
                    bw.newLine();
                    bw.write(AUTO_CONFIGURATION_END_MARKER);
                    bw.newLine();
                }
            });
        } catch (Throwable t) {
            try {
                if (Files.exists(keystoreBackupPath)) {
                    Files.move(keystoreBackupPath, keystorePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } else {
                    // this removes a statically named file, so it is potentially dangerous
                    Files.deleteIfExists(keystorePath);
                }
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            try {
                // this removes a statically named directory, so it is potentially dangerous
                deleteDirectory(env.configFile().resolve(TLS_GENERATED_CERTS_DIR_NAME));
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            Path backupCertsDir = env.configFile()
                .resolve(
                    String.format(Locale.ROOT, TLS_GENERATED_CERTS_DIR_NAME + ".%d.orig", autoConfigDate.toInstant().getEpochSecond())
                );
            if (Files.exists(backupCertsDir)) {
                moveDirectory(backupCertsDir, env.configFile().resolve(TLS_GENERATED_CERTS_DIR_NAME));
            }
            throw t;
        }
        // only delete the backed-up keystore file if all went well, because the new keystore contains its entries
        if (Files.exists(keystoreBackupPath)) {
            Files.delete(keystoreBackupPath);
        }
    }

    private static String initialMasterNodesSettingValue(Environment environment) {
        if (NODE_NAME_SETTING.exists(environment.settings())) {
            return "[\"" + NODE_NAME_SETTING.get(environment.settings()) + "\"]";
        }
        return "[\"${HOSTNAME}\"]";
    }

    /**
     * Determines if a node that is enrolling to an existing cluster is on a different host than the other nodes of the
     * cluster. If this is the case, then the default configuration of
     * binding transport layer to localhost will prevent this node to join the cluster even after "successful" enrollment.
     * We check the non-localhost transport addresses that we receive during enrollment and if any of these are not in the
     * list of non-localhost IP addresses that we gather from all interfaces of the current host, we assume that at least
     * some other node in the cluster runs on another host.
     * If the transport layer addresses we found out in enrollment are all localhost, we cannot be sure where we are still
     * on the same host, but we assume that as it is safer to do so and do not bind to non localhost for this node either.
     */
    protected static boolean anyRemoteHostNodeAddress(List<String> allNodesTransportPublishAddresses, InetAddress[] allHostAddresses) {
        final List<InetAddress> allAddressesList = Arrays.asList(allHostAddresses);
        for (String nodeStringAddress : allNodesTransportPublishAddresses) {
            try {
                final URI uri = new URI("http://" + nodeStringAddress);
                final InetAddress nodeAddress = InetAddress.getByName(uri.getHost());
                if (false == nodeAddress.isLoopbackAddress() && false == allAddressesList.contains(nodeAddress)) {
                    // this node's address is on a remote host
                    return true;
                }
            } catch (URISyntaxException | UnknownHostException e) {
                // we could fail here but if any of the transport addresses are usable, we can join the cluster
            }
        }
        return false;
    }

    private Environment possiblyReconfigureNode(Environment env, Terminal terminal, OptionSet options, ProcessInfo processInfo)
        throws UserException {
        // We remove the existing auto-configuration stanza from elasticsearch.yml, the elastisearch.keystore and
        // the directory with the auto-configured TLS key material, and then proceed as if elasticsearch is started
        // with --enrolment-token token, in the first place.
        final List<String> existingConfigLines;
        try {
            existingConfigLines = Files.readAllLines(env.configFile().resolve("elasticsearch.yml"), StandardCharsets.UTF_8);
        } catch (IOException e) {
            // This shouldn't happen, we would have failed earlier but we need to catch the exception
            throw new UserException(ExitCodes.IO_ERROR, "Aborting enrolling to cluster. Unable to read elasticsearch.yml.", e);
        }
        final List<String> existingConfigWithoutAutoconfiguration = removePreviousAutoconfiguration(existingConfigLines);
        if (false == existingConfigLines.equals(existingConfigWithoutAutoconfiguration)
            && Files.exists(env.configFile().resolve(TLS_GENERATED_CERTS_DIR_NAME))) {
            terminal.println("");
            terminal.println("This node will be reconfigured to join an existing cluster, using the enrollment token that you provided.");
            terminal.println("This operation will overwrite the existing configuration. Specifically: ");
            terminal.println("  - Security auto configuration will be removed from elasticsearch.yml");
            terminal.println("  - The [" + TLS_GENERATED_CERTS_DIR_NAME + "] config directory will be removed");
            terminal.println("  - Security auto configuration related secure settings will be removed from the elasticsearch.keystore");
            final boolean shouldContinue = terminal.promptYesNo("Do you want to continue with the reconfiguration process", false);
            if (shouldContinue == false) {
                throw new UserException(ExitCodes.OK, "User cancelled operation");
            }
            removeAutoConfigurationFromKeystore(env, terminal);
            try {
                fullyWriteFile(env.configFile(), "elasticsearch.yml", true, stream -> {
                    try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))) {
                        for (String l : existingConfigWithoutAutoconfiguration) {
                            bw.write(l);
                            bw.newLine();
                        }
                    }
                });
                deleteDirectory(env.configFile().resolve(TLS_GENERATED_CERTS_DIR_NAME));
            } catch (Throwable t) {
                throw new UserException(
                    ExitCodes.IO_ERROR,
                    "Aborting enrolling to cluster. Unable to remove existing security configuration.",
                    t
                );
            }
            // rebuild the environment after removing the settings that were added in auto-configuration.
            return createEnv(options, processInfo);
        } else {
            throw new UserException(
                ExitCodes.USAGE,
                "Aborting enrolling to cluster. This node doesn't appear to be auto-configured for security. "
                    + "Expected configuration is missing from elasticsearch.yml."
            );
        }
    }

    private static void notifyOfFailure(
        boolean inEnrollmentMode,
        Terminal terminal,
        Terminal.Verbosity verbosity,
        int exitCode,
        String message
    ) throws UserException {
        if (inEnrollmentMode) {
            throw new UserException(exitCode, message);
        } else {
            terminal.println(verbosity, message);
            throw new UserException(exitCode, null);
        }
    }

    private static void deleteDirectory(Path directory) throws IOException {
        IOUtils.rm(directory);
    }

    private static void moveDirectory(Path srcDir, Path dstDir) throws IOException {
        try {
            Files.move(srcDir, dstDir, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(srcDir, dstDir);
        }
    }

    private static GeneralNames getSubjectAltNames(Settings settings) throws IOException {
        Set<GeneralName> generalNameSet = new HashSet<>();
        for (InetAddress ip : NetworkUtils.getAllAddresses()) {
            String ipString = NetworkAddress.format(ip);
            generalNameSet.add(new GeneralName(GeneralName.iPAddress, ipString));
        }
        generalNameSet.add(new GeneralName(GeneralName.dNSName, "localhost"));
        // HOSTNAME should always be set by start-up scripts, and this code is always invoked only by the said startup scripts
        generalNameSet.add(new GeneralName(GeneralName.dNSName, System.getenv("HOSTNAME")));
        for (List<String> publishAddresses : List.of(
            NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.exists(settings)
                ? NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings)
                : List.<String>of(),
            HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST.exists(settings)
                ? HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST.get(settings)
                : List.<String>of()
        )) {
            for (String publishAddress : publishAddresses) {
                if (InetAddresses.isInetAddress(publishAddress)) {
                    generalNameSet.add(new GeneralName(GeneralName.iPAddress, publishAddress));
                } else {
                    generalNameSet.add(new GeneralName(GeneralName.dNSName, publishAddress));
                }
            }
        }
        return new GeneralNames(generalNameSet.toArray(new GeneralName[0]));
    }

    // for tests
    SecureString newKeystorePassword() {
        return UUIDs.randomBase64UUIDSecureString();
    }

    /*
     * Detect if the existing yml configuration is incompatible with auto-configuration,
     * in which case auto-configuration is SILENTLY skipped.
     * This assumes the user knows what they are doing when configuring the node.
     */
    void checkExistingConfiguration(Settings settings, boolean inEnrollmentMode, Terminal terminal) throws UserException {
        // Allow the user to explicitly set that they don't want auto-configuration for security, regardless of our heuristics
        if (XPackSettings.SECURITY_AUTOCONFIGURATION_ENABLED.get(settings) == false) {
            notifyOfFailure(
                inEnrollmentMode,
                terminal,
                Terminal.Verbosity.VERBOSE,
                ExitCodes.NOOP,
                "Skipping security auto configuration because [" + XPackSettings.SECURITY_AUTOCONFIGURATION_ENABLED.getKey() + "] is false"
            );
        }
        // Skip security auto configuration when Security is already configured.
        // Security is enabled implicitly, but if the admin chooses to enable it explicitly then
        // skip the TLS auto-configuration, as this is a sign that the admin is opting for the default 7.x behavior
        if (XPackSettings.SECURITY_ENABLED.exists(settings)) {
            // do not try to validate, correct or fill in any incomplete security configuration,
            // instead rely on the regular node startup to do this validation
            notifyOfFailure(
                inEnrollmentMode,
                terminal,
                Terminal.Verbosity.VERBOSE,
                ExitCodes.NOOP,
                "Skipping security auto configuration because it appears that security is already configured."
            );
        }

        // Skipping security auto configuration because TLS is already configured
        if (false == settings.getByPrefix(XPackSettings.TRANSPORT_SSL_PREFIX).isEmpty()
            || false == settings.getByPrefix(XPackSettings.HTTP_SSL_PREFIX).isEmpty()) {
            // zero validation for the TLS settings as well, let the node boot and do its thing
            notifyOfFailure(
                inEnrollmentMode,
                terminal,
                Terminal.Verbosity.VERBOSE,
                ExitCodes.NOOP,
                "Skipping security auto configuration because it appears that TLS is already configured."
            );
        }

        // Security auto configuration must not run if the node is configured for multi-node cluster formation (bootstrap or join).
        // This is because transport TLS with newly generated certs will hinder cluster formation because the other nodes cannot trust yet.
        if (false == isInitialClusterNode(settings)) {
            notifyOfFailure(
                inEnrollmentMode,
                terminal,
                Terminal.Verbosity.VERBOSE,
                ExitCodes.NOOP,
                "Skipping security auto configuration because this node is configured to bootstrap or to join a "
                    + "multi-node cluster, which is not supported."
            );
        }

        if (inEnrollmentMode == false) {
            // Silently skip security auto configuration because node cannot become master.
            boolean canBecomeMaster = DiscoveryNode.isMasterNode(settings)
                && false == DiscoveryNode.hasRole(settings, DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE);
            if (false == canBecomeMaster) {
                terminal.println(
                    Terminal.Verbosity.VERBOSE,
                    "Skipping security auto configuration because the node is configured such that it cannot become master."
                );
                throw new UserException(ExitCodes.NOOP, null);
            }
            // Silently skip security auto configuration, because the node cannot contain the Security index data
            boolean canHoldSecurityIndex = DiscoveryNode.canContainData(settings);
            if (false == canHoldSecurityIndex) {
                terminal.println(
                    Terminal.Verbosity.VERBOSE,
                    "Skipping security auto configuration because the node is configured such that it cannot contain data."
                );
                throw new UserException(ExitCodes.NOOP, null);
            }
        }

        // auto-configuration runs even if the realms are configured in any way,
        // including defining file based users (defining realms is permitted without touching
        // the xpack.security.enabled setting)
        // but the file realm is required for some of the auto-configuration parts (setting/resetting the elastic user)
        // if disabled, it must be manually enabled back and, preferably, at the head of the realm chain
    }

    // Unfortunately, we cannot tell, for every configuration, if it is going to result in a multi node cluster, as it depends
    // on the addresses that this node, and the others, will bind to when starting (and this runs on a single node before it
    // starts).
    // Here we take a conservative approach: if any of the discovery or initial master nodes setting are set to a non-empty
    // value, we assume the admin intended a multi-node cluster configuration. There is only one exception: if the initial master
    // nodes setting contains just the current node name.
    private static boolean isInitialClusterNode(Settings settings) {
        return DiscoveryModule.isSingleNodeDiscovery(settings)
            || (ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(settings).isEmpty()
                && SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING.get(settings).isEmpty()
                && DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.get(settings).isEmpty())
            || ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(settings).equals(List.of(NODE_NAME_SETTING.get(settings)));
    }

    private static void fullyWriteFile(
        Path basePath,
        String fileName,
        boolean replace,
        @Nullable String groupOwner,
        CheckedConsumer<OutputStream, Exception> writer
    ) throws Exception {
        Path filePath = basePath.resolve(fileName);
        if (false == replace && Files.exists(filePath)) {
            throw new UserException(
                ExitCodes.IO_ERROR,
                String.format(Locale.ROOT, "Output file [%s] already exists and " + "will not be replaced", filePath)
            );
        }
        // the default permission, if not replacing; if replacing use the permission of the to be replaced file
        Set<PosixFilePermission> permission = PosixFilePermissions.fromString("rw-rw----");
        // if replacing, use the permission of the replaced file
        if (Files.exists(filePath)) {
            PosixFileAttributeView view = Files.getFileAttributeView(filePath, PosixFileAttributeView.class);
            if (view != null) {
                permission = view.readAttributes().permissions();
            }
        }
        Path tmpPath = basePath.resolve(fileName + "." + UUIDs.randomBase64UUID() + ".tmp");
        try (OutputStream outputStream = Files.newOutputStream(tmpPath, StandardOpenOption.CREATE_NEW)) {
            writer.accept(outputStream);
        }
        // close file before moving, otherwise the windows FS throws IOException
        try {
            PosixFileAttributeView view = Files.getFileAttributeView(tmpPath, PosixFileAttributeView.class);
            if (view != null) {
                view.setPermissions(permission);
                if (null != groupOwner) {
                    UserPrincipalLookupService lookupService = PathUtils.getDefaultFileSystem().getUserPrincipalLookupService();
                    GroupPrincipal groupPrincipal = lookupService.lookupPrincipalByGroupName(groupOwner);
                    view.setGroup(groupPrincipal);
                }
            }
            if (replace) {
                Files.move(tmpPath, filePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } else {
                Files.move(tmpPath, filePath, StandardCopyOption.ATOMIC_MOVE);
            }

        } finally {
            Files.deleteIfExists(tmpPath);
        }
    }

    private static void fullyWriteFile(Path basePath, String fileName, boolean replace, CheckedConsumer<OutputStream, Exception> writer)
        throws Exception {
        fullyWriteFile(basePath, fileName, replace, null, writer);
    }

    private static boolean isDirEmpty(Path path) throws IOException {
        // Files.list MUST always be used in a try-with-resource construct in order to release the dir file handler
        try (Stream<Path> dirContentsStream = Files.list(path)) {
            return false == dirContentsStream.findAny().isPresent();
        }
    }

    private static X509Certificate parseCertificateFromPem(String pemFormattedCert, Terminal terminal) throws Exception {
        try {
            final List<Certificate> certs = CertParsingUtils.readCertificates(
                Base64.getDecoder().wrap(new ByteArrayInputStream(pemFormattedCert.getBytes(StandardCharsets.UTF_8)))
            );
            if (certs.size() != 1) {
                throw new IllegalStateException("Enroll node API returned multiple certificates");
            }
            return (X509Certificate) certs.get(0);
        } catch (Exception e) {
            terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
            terminal.errorPrintln(
                Terminal.Verbosity.VERBOSE,
                "Failed to parse Certificate from the response of the Enroll Node API: " + e.getMessage()
            );
            terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
            terminal.errorPrintln(Terminal.Verbosity.VERBOSE, ExceptionsHelper.stackTrace(e));
            terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
            throw new UserException(
                ExitCodes.DATA_ERROR,
                "Aborting enrolling to cluster. Failed to parse Certificate from the response of the Enroll Node API",
                e
            );
        }
    }

    private static PrivateKey parseKeyFromPem(String pemFormattedKey, Terminal terminal) throws UserException {
        try {
            return parsePKCS8PemString(pemFormattedKey);
        } catch (Exception e) {
            terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
            terminal.errorPrintln(
                Terminal.Verbosity.VERBOSE,
                "Failed to parse Private Key from the response of the Enroll Node API: " + e.getMessage()
            );
            terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
            terminal.errorPrintln(Terminal.Verbosity.VERBOSE, ExceptionsHelper.stackTrace(e));
            terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
            throw new UserException(
                ExitCodes.DATA_ERROR,
                "Aborting enrolling to cluster. Failed to parse Private Key from the response of the Enroll Node API",
                e
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> getTransportAddresses(Map<String, Object> responseMap) {
        return (List<String>) responseMap.get("nodes_addresses");
    }

    /**
     * Removes existing autoconfiguration from a configuration file, retaining configuration that is user added even within
     * the auto-configuration stanza. It only matches configuration setting keys for auto-configuration as the values depend
     * on the timestamp of the installation time so we can't know with certainty that a user has changed the value
     */
    static List<String> removePreviousAutoconfiguration(List<String> existingConfigLines) throws UserException {
        final Pattern pattern = Pattern.compile(
            "(?s)(" + Pattern.quote(AUTO_CONFIGURATION_START_MARKER) + ".*?" + Pattern.quote(AUTO_CONFIGURATION_END_MARKER) + ")"
        );
        final String existingConfigurationAsString = existingConfigLines.stream().collect(Collectors.joining(System.lineSeparator()));
        final Matcher matcher = pattern.matcher(existingConfigurationAsString);
        if (matcher.find()) {
            final String foundAutoConfigurationSettingsAsString = matcher.group(1);
            final Settings foundAutoConfigurationSettings;
            try {
                foundAutoConfigurationSettings = Settings.builder()
                    .loadFromSource(foundAutoConfigurationSettingsAsString, XContentType.YAML)
                    .build();
            } catch (Exception e) {
                throw new UserException(
                    ExitCodes.IO_ERROR,
                    "Aborting enrolling to cluster. Unable to parse existing configuration file. Error was: " + e.getMessage(),
                    e
                );
            }
            // This is brittle and needs to be updated with every change above
            final Set<String> expectedAutoConfigurationSettings = new TreeSet<String>(
                List.of(
                    "xpack.security.enabled",
                    "xpack.security.enrollment.enabled",
                    "xpack.security.transport.ssl.keystore.path",
                    "xpack.security.transport.ssl.truststore.path",
                    "xpack.security.transport.ssl.verification_mode",
                    "xpack.security.http.ssl.enabled",
                    "xpack.security.transport.ssl.enabled",
                    "xpack.security.http.ssl.keystore.path",
                    "cluster.initial_master_nodes",
                    "http.host"
                )
            );
            final Set<String> userAddedSettings = new HashSet<>(foundAutoConfigurationSettings.keySet());
            userAddedSettings.removeAll(expectedAutoConfigurationSettings);
            final List<String> newConfigurationLines = Arrays.stream(
                existingConfigurationAsString.replace(foundAutoConfigurationSettingsAsString, "").split(System.lineSeparator())
            ).collect(Collectors.toList());
            if (false == userAddedSettings.isEmpty()) {
                for (String key : userAddedSettings) {
                    newConfigurationLines.add(key + ": " + foundAutoConfigurationSettings.get(key));
                }
            }
            return newConfigurationLines;
        }
        return existingConfigLines;
    }

    private static void removeAutoConfigurationFromKeystore(Environment env, Terminal terminal) throws UserException {
        if (Files.exists(KeyStoreWrapper.keystorePath(env.configFile()))) {
            try (
                KeyStoreWrapper existingKeystore = KeyStoreWrapper.load(env.configFile());
                SecureString keystorePassword = existingKeystore.hasPassword()
                    ? new SecureString(terminal.readSecret("Enter password for the elasticsearch keystore: "))
                    : new SecureString(new char[0]);
            ) {
                existingKeystore.decrypt(keystorePassword.getChars());
                List<String> secureSettingsToRemove = List.of(
                    "xpack.security.transport.ssl.keystore.secure_password",
                    "xpack.security.transport.ssl.truststore.secure_password",
                    "xpack.security.http.ssl.keystore.secure_password",
                    "autoconfiguration.password_hash"
                );
                for (String setting : secureSettingsToRemove) {
                    if (existingKeystore.getSettingNames().contains(setting) == false) {
                        throw new UserException(
                            ExitCodes.IO_ERROR,
                            "Aborting enrolling to cluster. Unable to remove existing security configuration, "
                                + "elasticsearch.keystore did not contain expected setting ["
                                + setting
                                + "]."
                        );
                    }
                    existingKeystore.remove(setting);
                }
                existingKeystore.save(env.configFile(), keystorePassword.getChars());
            } catch (Exception e) {
                terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
                terminal.errorPrintln(Terminal.Verbosity.VERBOSE, ExceptionsHelper.stackTrace(e));
                terminal.errorPrintln(Terminal.Verbosity.VERBOSE, "");
                throw new UserException(
                    ExitCodes.IO_ERROR,
                    "Aborting enrolling to cluster. Unable to remove existing secure settings. Error was: " + e.getMessage(),
                    e
                );
            }
        }
    }

}
