/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.util.CollectionUtil;
//import org.bouncycastle.bcpg.ArmoredInputStream;
//import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
//import org.bouncycastle.openpgp.PGPException;
//import org.bouncycastle.openpgp.PGPPublicKey;
//import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
//import org.bouncycastle.openpgp.PGPSignature;
//import org.bouncycastle.openpgp.PGPSignatureList;
//import org.bouncycastle.openpgp.PGPUtil;
//import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
//import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
//import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.PluginPolicyInfo;
import org.elasticsearch.bootstrap.PolicyUtil;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.plugins.PluginsService;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Permission;
import java.security.UnresolvedPermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * An action for installing plugins into Elasticsearch.
 * <p>
 * The install action takes a number of plugin descriptors. Each contains an ID, which may be any of the following:
 * <ul>
 *     <li>An official elasticsearch plugin name</li>
 *     <li>Maven coordinates to a plugin zip</li>
 *     <li>A URL to a plugin zip</li>
 * </ul>
 * <p>
 * Plugins are packaged as zip files. Each packaged plugin must contain a plugin properties file.
 * See {@link PluginInfo}.
 * <p>
 * The installation process first extracts the plugin files into a temporary
 * directory in order to verify the plugin satisfies the following requirements:
 * <ul>
 *     <li>Jar hell does not exist, either between the plugin's own jars, or with elasticsearch</li>
 *     <li>The plugin is not a module already provided with elasticsearch</li>
 *     <li>If the plugin contains extra security permissions, the policy file is validated</li>
 * </ul>
 * <p>
 * A plugin used to be able to also contain an optional {@code bin} directory which contains scripts.
 * This is not supported in this class.
 * <p>
 * A plugin may also contain an optional {@code config} directory which contains configuration
 * files specific to the plugin. The config files be installed into a subdirectory of the
 * elasticsearch config directory, using the name of the plugin. If any files to be installed
 * already exist, they will be skipped.
 */
class PluginInstaller implements Closeable {

    private static final String PROPERTY_STAGING_ID = "es.plugins.staging";

    static final Set<PosixFilePermission> CONFIG_DIR_PERMS;
    static final Set<PosixFilePermission> CONFIG_FILES_PERMS;
    static final Set<PosixFilePermission> PLUGIN_DIR_PERMS;
    static final Set<PosixFilePermission> PLUGIN_FILES_PERMS;

    static {
        // Config directory get chmod 750
        CONFIG_DIR_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rwxr-x---"));

        // Config files get chmod 660
        CONFIG_FILES_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rw-rw----"));

        // Plugin directory get chmod 755
        PLUGIN_DIR_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rwxr-xr-x"));

        // Plugins files get chmod 644
        PLUGIN_FILES_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rw-r--r--"));
    }

    private final Set<String> modules;
    private final Set<String> officialPlugins;
    private final Logger logger;
    private Environment env;
    private Proxy proxy = Proxy.NO_PROXY;

    private final List<Path> pathsToDeleteOnShutdown = new ArrayList<>();

    PluginInstaller(Environment env, Set<String> modules, Set<String> officialPlugins) {
        this.env = env;
        this.modules = modules;
        this.officialPlugins = officialPlugins;

        this.logger = LogManager.getLogger(this.getClass());
    }

    // pkg private for testing
    void execute(List<PluginDescriptor> plugins) throws Exception {
        if (plugins.isEmpty()) {
            throw new PluginSyncException("at least one plugin id is required");
        }

        final Set<String> uniquePluginIds = new HashSet<>();
        for (final PluginDescriptor plugin : plugins) {
            if (uniquePluginIds.add(plugin.getId()) == false) {
                throw new PluginSyncException("duplicate plugin id [" + plugin.getId() + "]");
            }
        }

        final Map<String, List<Path>> deleteOnFailures = new LinkedHashMap<>();
        for (final PluginDescriptor descriptor : plugins) {
            final String pluginId = descriptor.getId();

            if ("x-pack".equals(pluginId)) {
                throw new PluginSyncException("this distribution of Elasticsearch contains X-Pack by default");
            }

            try {
                this.logger.info("-> Installing {}", pluginId);
                final List<Path> deleteOnFailure = new ArrayList<>();
                deleteOnFailures.put(pluginId, deleteOnFailure);

                final Path pluginZip = download(descriptor, env.tmpFile());
                final Path extractedZip = unzip(pluginZip, env.pluginsFile());
                deleteOnFailure.add(extractedZip);
                final PluginInfo pluginInfo = installPlugin(descriptor, extractedZip, deleteOnFailure);
                this.logger.info("-> Installed {}", pluginInfo.getName());
                // swap the entry by plugin id for one with the installed plugin name, it gives a cleaner error message for URL installs
                deleteOnFailures.remove(pluginId);
                deleteOnFailures.put(pluginInfo.getName(), deleteOnFailure);
            } catch (final Exception installProblem) {
                this.logger.warn("-> Failed installing {}", pluginId);

                for (final Map.Entry<String, List<Path>> deleteOnFailureEntry : deleteOnFailures.entrySet()) {
                    this.logger.warn("-> Rolling back {}", deleteOnFailureEntry.getKey());
                    boolean success = false;
                    try {
                        IOUtils.rm(deleteOnFailureEntry.getValue().toArray(new Path[0]));
                        success = true;
                    } catch (final IOException exceptionWhileRemovingFiles) {
                        final PluginSyncException exception = new PluginSyncException(
                            "failed rolling back installation of [" + deleteOnFailureEntry.getKey() + "]",
                            exceptionWhileRemovingFiles
                        );
                        installProblem.addSuppressed(exception);
                        this.logger.warn("-> Failed rolling back {}", deleteOnFailureEntry.getKey());
                    }
                    if (success) {
                        this.logger.warn("-> Rolled back {}", deleteOnFailureEntry.getKey());
                    }
                }
                throw installProblem;
            }
        }
    }

    /**
     * Downloads the plugin and returns the file it was downloaded to.
     */
    private Path download(PluginDescriptor plugin, Path tmpDir) throws Exception {
        final String pluginId = plugin.getId();

        if (this.officialPlugins.contains(pluginId) && plugin.getLocation() == null) {
            final String pluginArchiveDir = System.getenv("ES_PLUGIN_ARCHIVE_DIR");
            if (pluginArchiveDir != null && pluginArchiveDir.isEmpty() == false) {
                final Path pluginPath = getPluginArchivePath(pluginId, pluginArchiveDir);
                if (Files.exists(pluginPath)) {
                    this.logger.info("-> Downloading {} from local archive: {}", pluginId, pluginArchiveDir);
                    return downloadZip("file://" + pluginPath, tmpDir);
                }
                // else carry on to regular download
            }

            final String url = getElasticUrl(getStagingHash(), Version.CURRENT, isSnapshot(), pluginId, Platforms.PLATFORM_NAME);
            this.logger.info("-> Downloading {} from elastic", pluginId);
            return downloadAndValidate(url, tmpDir, true);
        }

        final String pluginLocation = plugin.getLocation();

        // now try as maven coordinates, a valid URL would only have a colon and slash
        String[] coordinates = pluginLocation.split(":");
        if (coordinates.length == 3 && pluginLocation.contains("/") == false && pluginLocation.startsWith("file:") == false) {
            String mavenUrl = getMavenUrl(coordinates);
            this.logger.info("-> Downloading {} from maven central", pluginId);
            return downloadAndValidate(mavenUrl, tmpDir, false);
        }

        // fall back to plain old URL
        if (pluginLocation.contains(":") == false) {
            // definitely not a valid url, so assume it is a plugin name
            List<String> pluginSuggestions = checkMisspelledPlugin(pluginId);
            String msg = "Unknown plugin " + pluginId;
            if (pluginSuggestions.isEmpty() == false) {
                msg += ", did you mean " + (pluginSuggestions.size() > 1 ? "any of " : "") + pluginSuggestions + "?";
            }
            throw new PluginSyncException(msg);
        }
        this.logger.info("-> Downloading {}", URLDecoder.decode(pluginLocation, StandardCharsets.UTF_8));
        return downloadZip(pluginLocation, tmpDir);
    }

    @SuppressForbidden(reason = "Need to use PathUtils#get")
    private Path getPluginArchivePath(String pluginId, String pluginArchiveDir) throws PluginSyncException {
        final Path path = PathUtils.get(pluginArchiveDir);
        if (Files.exists(path) == false) {
            throw new PluginSyncException("Location in ES_PLUGIN_ARCHIVE_DIR does not exist");
        }
        if (Files.isDirectory(path) == false) {
            throw new PluginSyncException("Location in ES_PLUGIN_ARCHIVE_DIR is not a directory");
        }
        return PathUtils.get(pluginArchiveDir, pluginId + "-" + Version.CURRENT + (isSnapshot() ? "-SNAPSHOT" : "") + ".zip");
    }

    // pkg private so tests can override
    String getStagingHash() {
        return System.getProperty(PROPERTY_STAGING_ID);
    }

    boolean isSnapshot() {
        return Build.CURRENT.isSnapshot();
    }

    /**
     * Returns the url for an official elasticsearch plugin.
     */
    private String getElasticUrl(
        final String stagingHash,
        final Version version,
        final boolean isSnapshot,
        final String pluginId,
        final String platform
    ) throws IOException, PluginSyncException {
        final String baseUrl;
        if (isSnapshot && stagingHash == null) {
            throw new PluginSyncException("attempted to install release build of official plugin on snapshot build of Elasticsearch");
        }
        if (stagingHash != null) {
            if (isSnapshot) {
                baseUrl = nonReleaseUrl("snapshots", version, stagingHash, pluginId);
            } else {
                baseUrl = nonReleaseUrl("staging", version, stagingHash, pluginId);
            }
        } else {
            baseUrl = String.format(Locale.ROOT, "https://artifacts.elastic.co/downloads/elasticsearch-plugins/%s", pluginId);
        }
        final String platformUrl = String.format(
            Locale.ROOT,
            "%s/%s-%s-%s.zip",
            baseUrl,
            pluginId,
            platform,
            Build.CURRENT.getQualifiedVersion()
        );
        if (urlExists(platformUrl)) {
            return platformUrl;
        }
        return String.format(Locale.ROOT, "%s/%s-%s.zip", baseUrl, pluginId, Build.CURRENT.getQualifiedVersion());
    }

    private String nonReleaseUrl(final String hostname, final Version version, final String stagingHash, final String pluginId) {
        return String.format(
            Locale.ROOT,
            "https://%s.elastic.co/%s-%s/downloads/elasticsearch-plugins/%s",
            hostname,
            version,
            stagingHash,
            pluginId
        );
    }

    /**
     * Returns the url for an elasticsearch plugin in maven.
     */
    private String getMavenUrl(String[] coordinates) throws IOException {
        final String groupId = coordinates[0].replace(".", "/");
        final String artifactId = coordinates[1];
        final String version = coordinates[2];
        final String baseUrl = String.format(Locale.ROOT, "https://repo1.maven.org/maven2/%s/%s/%s", groupId, artifactId, version);
        final String platformUrl = String.format(Locale.ROOT, "%s/%s-%s-%s.zip", baseUrl, artifactId, Platforms.PLATFORM_NAME, version);
        if (urlExists(platformUrl)) {
            return platformUrl;
        }
        return String.format(Locale.ROOT, "%s/%s-%s.zip", baseUrl, artifactId, version);
    }

    /**
     * Returns {@code true} if the given url exists, and {@code false} otherwise.
     * <p>
     * The given url must be {@code https} and existing means a {@code HEAD} request returns 200.
     */
    // pkg private for tests to manipulate
    @SuppressForbidden(reason = "Make HEAD request using URLConnection.connect()")
    boolean urlExists(String urlString) throws IOException {
        this.logger.debug("Checking if url exists: " + urlString);
        URL url = new URL(urlString);
        assert "https".equals(url.getProtocol()) : "Only http urls can be checked";
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.addRequestProperty("User-Agent", "elasticsearch-plugin-installer");
        urlConnection.setRequestMethod("HEAD");
        urlConnection.connect();
        return urlConnection.getResponseCode() == 200;
    }

    /**
     * Returns all the official plugin names that look similar to pluginId.
     **/
    private List<String> checkMisspelledPlugin(String pluginId) {
        LevenshteinDistance ld = new LevenshteinDistance();
        List<Tuple<Float, String>> scoredKeys = new ArrayList<>();
        for (String officialPlugin : this.officialPlugins) {
            float distance = ld.getDistance(pluginId, officialPlugin);
            if (distance > 0.7f) {
                scoredKeys.add(new Tuple<>(distance, officialPlugin));
            }
        }
        CollectionUtil.timSort(scoredKeys, (a, b) -> b.v1().compareTo(a.v1()));
        return scoredKeys.stream().map(Tuple::v2).collect(Collectors.toList());
    }

    /** Downloads a zip from the url, into a temp file under the given temp dir. */
    // pkg private for tests
    @SuppressForbidden(reason = "We use getInputStream to download plugins")
    Path downloadZip(String urlString, Path tmpDir) throws IOException {
        this.logger.debug("Retrieving zip from " + urlString);
        URL url = new URL(urlString);
        Path zip = Files.createTempFile(tmpDir, null, ".zip");
        URLConnection urlConnection = url.openConnection(this.proxy);
        urlConnection.addRequestProperty("User-Agent", "elasticsearch-plugin-installer");
        try (InputStream in = urlConnection.getInputStream()) {
            // must overwrite since creating the temp file above actually created the file
            Files.copy(in, zip, StandardCopyOption.REPLACE_EXISTING);
        }
        return zip;
    }

    // for testing only
    void setEnvironment(Environment env) {
        this.env = env;
    }

    void setProxy(Proxy proxy) {
        this.proxy = Objects.requireNonNull(proxy);
    }

    @SuppressForbidden(reason = "URL#openConnection")
    InputStream urlOpenStream(final URL url) throws IOException {
        return url.openConnection(this.proxy).getInputStream();
    }

    /**
     * Downloads a ZIP from the URL. This method also validates the downloaded plugin ZIP via the following means:
     * <ul>
     * <li>
     * For an official plugin we download the SHA-512 checksum and validate the integrity of the downloaded ZIP. We also download the
     * armored signature and validate the authenticity of the downloaded ZIP.
     * </li>
     * <li>
     * For a non-official plugin we download the SHA-512 checksum and fallback to the SHA-1 checksum and validate the integrity of the
     * downloaded ZIP.
     * </li>
     * </ul>
     *
     * @param urlString      the URL of the plugin ZIP
     * @param tmpDir         a temporary directory to write downloaded files to
     * @param officialPlugin true if the plugin is an official plugin
     * @return the path to the downloaded plugin ZIP
     * @throws IOException   if an I/O exception occurs download or reading files and resources
     * @throws PluginSyncException if checksum validation fails
     */
    private Path downloadAndValidate(final String urlString, final Path tmpDir, final boolean officialPlugin) throws IOException,
        PluginSyncException {
        Path zip = downloadZip(urlString, tmpDir);
        pathsToDeleteOnShutdown.add(zip);
        String checksumUrlString = urlString + ".sha512";
        URL checksumUrl = openUrl(checksumUrlString);
        String digestAlgo = "SHA-512";
        if (checksumUrl == null && officialPlugin == false) {
            // fallback to sha1, until 7.0, but with warning
            this.logger.warn(
                "Warning: sha512 not found, falling back to sha1. This behavior is deprecated and will be removed in a "
                    + "future release. Please update the plugin to use a sha512 checksum."
            );
            checksumUrlString = urlString + ".sha1";
            checksumUrl = openUrl(checksumUrlString);
            digestAlgo = "SHA-1";
        }
        if (checksumUrl == null) {
            throw new PluginSyncException("Plugin checksum missing: " + checksumUrlString);
        }
        final String expectedChecksum;
        try (InputStream in = urlOpenStream(checksumUrl)) {
            /*
             * The supported format of the SHA-1 files is a single-line file containing the SHA-1. The supported format of the SHA-512 files
             * is a single-line file containing the SHA-512 and the filename, separated by two spaces. For SHA-1, we verify that the hash
             * matches, and that the file contains a single line. For SHA-512, we verify that the hash and the filename match, and that the
             * file contains a single line.
             */
            final BufferedReader checksumReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            if (digestAlgo.equals("SHA-1")) {
                expectedChecksum = checksumReader.readLine();
            } else {
                final String checksumLine = checksumReader.readLine();
                final String[] fields = checksumLine.split(" {2}");
                if (officialPlugin && fields.length != 2 || officialPlugin == false && fields.length > 2) {
                    throw new PluginSyncException("Invalid checksum file at " + checksumUrl);
                }
                expectedChecksum = fields[0];
                if (fields.length == 2) {
                    // checksum line contains filename as well
                    final String[] segments = URI.create(urlString).getPath().split("/");
                    final String expectedFile = segments[segments.length - 1];
                    if (fields[1].equals(expectedFile) == false) {
                        final String message = String.format(
                            Locale.ROOT,
                            "checksum file at [%s] is not for this plugin, expected [%s] but was [%s]",
                            checksumUrl,
                            expectedFile,
                            fields[1]
                        );
                        throw new PluginSyncException(message);
                    }
                }
            }
            if (checksumReader.readLine() != null) {
                throw new PluginSyncException("Invalid checksum file at " + checksumUrl);
            }
        }

        // read the bytes of the plugin zip in chunks to avoid out of memory errors
        try (InputStream zis = Files.newInputStream(zip)) {
            try {
                final MessageDigest digest = MessageDigest.getInstance(digestAlgo);
                final byte[] bytes = new byte[8192];
                int read;
                while ((read = zis.read(bytes)) != -1) {
                    assert read > 0 : read;
                    digest.update(bytes, 0, read);
                }
                final String actualChecksum = MessageDigests.toHexString(digest.digest());
                if (expectedChecksum.equals(actualChecksum) == false) {
                    throw new PluginSyncException(digestAlgo + " mismatch, expected " + expectedChecksum + " but got " + actualChecksum);
                }
            } catch (final NoSuchAlgorithmException e) {
                // this should never happen as we are using SHA-1 and SHA-512 here
                throw new AssertionError(e);
            }
        }

//        if (officialPlugin) {
//            verifySignature(zip, urlString);
//        }

        return zip;
    }

//    /**
//     * Verify the signature of the downloaded plugin ZIP. The signature is obtained from the source of the downloaded plugin by appending
//     * ".asc" to the URL. It is expected that the plugin is signed with the Elastic signing key with ID D27D666CD88E42B4.
//     *
//     * @param zip       the path to the downloaded plugin ZIP
//     * @param urlString the URL source of the downloaded plugin ZIP
//     * @throws IOException  if an I/O exception occurs reading from various input streams
//     * @throws PGPException if the PGP implementation throws an internal exception during verification
//     */
//    void verifySignature(final Path zip, final String urlString) throws IOException, PGPException {
//        final String ascUrlString = urlString + ".asc";
//        final URL ascUrl = openUrl(ascUrlString);
//        try (
//            // fin is a file stream over the downloaded plugin zip whose signature to verify
//            InputStream fin = pluginZipInputStream(zip);
//            // sin is a URL stream to the signature corresponding to the downloaded plugin zip
//            InputStream sin = urlOpenStream(ascUrl);
//            // ain is a input stream to the public key in ASCII-Armor format (RFC4880)
//            InputStream ain = new ArmoredInputStream(getPublicKey())
//        ) {
//            final JcaPGPObjectFactory factory = new JcaPGPObjectFactory(PGPUtil.getDecoderStream(sin));
//            final PGPSignature signature = ((PGPSignatureList) factory.nextObject()).get(0);
//
//            // validate the signature has key ID matching our public key ID
//            final String keyId = Long.toHexString(signature.getKeyID()).toUpperCase(Locale.ROOT);
//            if (getPublicKeyId().equals(keyId) == false) {
//                throw new IllegalStateException("key id [" + keyId + "] does not match expected key id [" + getPublicKeyId() + "]");
//            }
//
//            // compute the signature of the downloaded plugin zip
//            final PGPPublicKeyRingCollection collection = new PGPPublicKeyRingCollection(ain, new JcaKeyFingerprintCalculator());
//            final PGPPublicKey key = collection.getPublicKey(signature.getKeyID());
//            signature.init(new JcaPGPContentVerifierBuilderProvider().setProvider(new BouncyCastleFipsProvider()), key);
//            final byte[] buffer = new byte[1024];
//            int read;
//            while ((read = fin.read(buffer)) != -1) {
//                signature.update(buffer, 0, read);
//            }
//
//            // finally we verify the signature of the downloaded plugin zip matches the expected signature
//            if (signature.verify() == false) {
//                throw new IllegalStateException("signature verification for [" + urlString + "] failed");
//            }
//        }
//    }

    /**
     * An input stream to the raw bytes of the plugin ZIP.
     *
     * @param zip the path to the downloaded plugin ZIP
     * @return an input stream to the raw bytes of the plugin ZIP.
     * @throws IOException if an I/O exception occurs preparing the input stream
     */
    InputStream pluginZipInputStream(final Path zip) throws IOException {
        return Files.newInputStream(zip);
    }

    /**
     * Return the public key ID of the signing key that is expected to have signed the official plugin.
     *
     * @return the public key ID
     */
    String getPublicKeyId() {
        return "D27D666CD88E42B4";
    }

    /**
     * An input stream to the public key of the signing key.
     *
     * @return an input stream to the public key
     */
    InputStream getPublicKey() {
        return PluginInstaller.class.getResourceAsStream("/public_key.asc");
    }

    /**
     * Creates a URL and opens a connection.
     * <p>
     * If the URL returns a 404, {@code null} is returned, otherwise the open URL object is returned.
     */
    // pkg private for tests
    URL openUrl(String urlString) throws IOException {
        URL checksumUrl = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) checksumUrl.openConnection(this.proxy);
        if (connection.getResponseCode() == 404) {
            return null;
        }
        return checksumUrl;
    }

    private Path unzip(Path zip, Path pluginsDir) throws IOException, PluginSyncException {
        // unzip plugin to a staging temp dir

        final Path target = stagingDirectory(pluginsDir);
        pathsToDeleteOnShutdown.add(target);

        try (ZipInputStream zipInput = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            byte[] buffer = new byte[8192];
            while ((entry = zipInput.getNextEntry()) != null) {
                if (entry.getName().startsWith("elasticsearch/")) {
                    throw new PluginSyncException(
                        "This plugin was built with an older plugin structure."
                            + " Contact the plugin author to remove the intermediate \"elasticsearch\" directory within the plugin zip."
                    );
                }
                Path targetFile = target.resolve(entry.getName());

                // Using the entry name as a path can result in an entry outside of the plugin dir,
                // either if the name starts with the root of the filesystem, or it is a relative
                // entry like ../whatever. This check attempts to identify both cases by first
                // normalizing the path (which removes foo/..) and ensuring the normalized entry
                // is still rooted with the target plugin directory.
                if (targetFile.normalize().startsWith(target) == false) {
                    throw new PluginSyncException(
                        "Zip contains entry name '" + entry.getName() + "' resolving outside of plugin directory"
                    );
                }

                // be on the safe side: do not rely on that directories are always extracted
                // before their children (although this makes sense, but is it guaranteed?)
                if (Files.isSymbolicLink(targetFile.getParent()) == false) {
                    Files.createDirectories(targetFile.getParent());
                }
                if (entry.isDirectory() == false) {
                    try (OutputStream out = Files.newOutputStream(targetFile)) {
                        int len;
                        while ((len = zipInput.read(buffer)) >= 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
                zipInput.closeEntry();
            }
        } catch (PluginSyncException e) {
            IOUtils.rm(target);
            throw e;
        }
        Files.delete(zip);
        return target;
    }

    private Path stagingDirectory(Path pluginsDir) throws IOException {
        try {
            return Files.createTempDirectory(pluginsDir, ".installing-", PosixFilePermissions.asFileAttribute(PLUGIN_DIR_PERMS));
        } catch (UnsupportedOperationException e) {
            return stagingDirectoryWithoutPosixPermissions(pluginsDir);
        }
    }

    private Path stagingDirectoryWithoutPosixPermissions(Path pluginsDir) throws IOException {
        return Files.createTempDirectory(pluginsDir, ".installing-");
    }

    // checking for existing version of the plugin
    private void verifyPluginName(Path pluginPath, String pluginName) throws PluginSyncException {
        // don't let user install plugin conflicting with module...
        // they might be unavoidably in maven central and are packaged up the same way)
        if (this.modules.contains(pluginName)) {
            throw new PluginSyncException("plugin '" + pluginName + "' cannot be installed as a plugin, it is a system module");
        }

        final Path destination = pluginPath.resolve(pluginName);
        if (Files.exists(destination)) {
            final String message = String.format(
                Locale.ROOT,
                "plugin directory [%s] already exists; if you need to update the plugin, uninstall it first using command 'remove %s'",
                destination,
                pluginName
            );
            throw new PluginSyncException(message);
        }
    }

    /**
     * Load information about the plugin, and verify it can be installed with no errors.
     */
    private PluginInfo loadPluginInfo(Path pluginRoot) throws Exception {
        final PluginInfo info = PluginInfo.readFromProperties(pluginRoot);
        if (info.hasNativeController()) {
            throw new IllegalStateException("plugins can not have native controllers");
        }
        PluginsService.verifyCompatibility(info);

        // checking for existing version of the plugin
        verifyPluginName(env.pluginsFile(), info.getName());

        PluginsService.checkForFailedPluginRemovals(env.pluginsFile());

        this.logger.info(info.toString());

        // check for jar hell before any copying
        jarHellCheck(info, pluginRoot, env.pluginsFile(), env.modulesFile());

        return info;
    }

    private static final String LIB_TOOLS_PLUGIN_CLI_CLASSPATH_JAR;

    static {
        LIB_TOOLS_PLUGIN_CLI_CLASSPATH_JAR = String.format(Locale.ROOT, ".+%1$slib%1$stools%1$splugin-cli%1$s[^%1$s]+\\.jar", "(/|\\\\)");
    }

    /**
     * check a candidate plugin for jar hell before installing it
     */
    void jarHellCheck(PluginInfo candidateInfo, Path candidateDir, Path pluginsDir, Path modulesDir) throws Exception {
        // create list of current jars in classpath
        final Set<URL> classpath = JarHell.parseClassPath().stream().filter(url -> {
            try {
                return url.toURI().getPath().matches(LIB_TOOLS_PLUGIN_CLI_CLASSPATH_JAR) == false;
            } catch (final URISyntaxException e) {
                throw new AssertionError(e);
            }
        }).collect(Collectors.toSet());

        // read existing bundles. this does some checks on the installation too.
        Set<PluginsService.Bundle> bundles = new HashSet<>(PluginsService.getPluginBundles(pluginsDir));
        bundles.addAll(PluginsService.getModuleBundles(modulesDir));
        bundles.add(new PluginsService.Bundle(candidateInfo, candidateDir));
        List<PluginsService.Bundle> sortedBundles = PluginsService.sortBundles(bundles);

        // check jarhell of all plugins so we know this plugin and anything depending on it are ok together
        // TODO: optimize to skip any bundles not connected to the candidate plugin?
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        for (PluginsService.Bundle bundle : sortedBundles) {
            PluginsService.checkBundleJarHell(classpath, bundle, transitiveUrls);
        }

        // TODO: no jars should be an error
        // TODO: verify the classname exists in one of the jars!
    }

    /**
     * Installs the plugin from {@code tmpRoot} into the plugins dir.
     * If the plugin has a bin dir and/or a config dir, those are moved.
     */
    private PluginInfo installPlugin(PluginDescriptor descriptor, Path tmpRoot, List<Path> deleteOnFailure) throws Exception {
        final PluginInfo info = loadPluginInfo(tmpRoot);
        PluginPolicyInfo pluginPolicy = PolicyUtil.getPluginPolicyInfo(tmpRoot, env.tmpFile());
        if (pluginPolicy != null) {
            Set<String> permissions = getPermissionDescriptions(pluginPolicy, env.tmpFile());
            this.logger.warn("NOTE: plugin {} requires extra permissions! {}", descriptor.getId(), permissions);
            this.logger.warn(
                "See http://docs.oracle.com/javase/8/docs/technotes/guides/security/permissions.html "
                    + "for descriptions of what these permissions allow and the associated risks."
            );
        }

        // Validate that the downloaded plugin's ID matches what we expect from the descriptor. The
        // exception is if we install a plugin via `InstallPluginCommand` by specifying a URL or
        // Maven coordinates, because then we can't know in advance what the plugin ID ought to be.
        if (descriptor.getId().contains(":") == false && descriptor.getId().equals(info.getName()) == false) {
            throw new PluginSyncException(
                "Expected downloaded plugin to have ID [" + descriptor.getId() + "] but found [" + info.getName() + "]"
            );
        }

        final Path destination = env.pluginsFile().resolve(info.getName());
        deleteOnFailure.add(destination);

        Path tmpConfigDir = tmpRoot.resolve("config");
        if (Files.exists(tmpConfigDir)) {
            // some files may already exist, and we don't remove plugin config files on plugin removal,
            // so any installed config files are left on failure too
            installConfig(info, tmpConfigDir, env.configFile().resolve(info.getName()));
        }

        movePlugin(tmpRoot, destination);
        return info;
    }

    /**
     * Extract a unique set of permissions from the plugin's policy file. Each permission is formatted for output to users.
     */
    static Set<String> getPermissionDescriptions(PluginPolicyInfo pluginPolicyInfo, Path tmpDir) throws IOException {
        Set<Permission> allPermissions = new HashSet<>(PolicyUtil.getPolicyPermissions(null, pluginPolicyInfo.policy, tmpDir));
        for (URL jar : pluginPolicyInfo.jars) {
            Set<Permission> jarPermissions = PolicyUtil.getPolicyPermissions(jar, pluginPolicyInfo.policy, tmpDir);
            allPermissions.addAll(jarPermissions);
        }

        return allPermissions.stream().map(PluginInstaller::formatPermission).collect(Collectors.toSet());
    }

    /** Format permission type, name, and actions into a string */
    static String formatPermission(Permission permission) {
        StringBuilder sb = new StringBuilder();

        String clazz = null;
        if (permission instanceof UnresolvedPermission) {
            clazz = ((UnresolvedPermission) permission).getUnresolvedType();
        } else {
            clazz = permission.getClass().getName();
        }
        sb.append(clazz);

        String name = null;
        if (permission instanceof UnresolvedPermission) {
            name = ((UnresolvedPermission) permission).getUnresolvedName();
        } else {
            name = permission.getName();
        }
        if (name != null && name.length() > 0) {
            sb.append(' ');
            sb.append(name);
        }

        String actions = null;
        if (permission instanceof UnresolvedPermission) {
            actions = ((UnresolvedPermission) permission).getUnresolvedActions();
        } else {
            actions = permission.getActions();
        }
        if (actions != null && actions.length() > 0) {
            sb.append(' ');
            sb.append(actions);
        }
        return sb.toString();
    }

    /**
     * Moves the plugin directory into its final destination.
     **/
    private void movePlugin(Path tmpRoot, Path destination) throws IOException {
        Files.move(tmpRoot, destination, StandardCopyOption.ATOMIC_MOVE);
        Files.walkFileTree(destination, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                setFileAttributes(file, PLUGIN_FILES_PERMS);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                setFileAttributes(dir, PLUGIN_DIR_PERMS);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Copies the files from {@code tmpConfigDir} into {@code destConfigDir}.
     * Any files existing in both the source and destination will be skipped.
     */
    private void installConfig(PluginInfo info, Path tmpConfigDir, Path destConfigDir) throws Exception {
        if (Files.isDirectory(tmpConfigDir) == false) {
            throw new PluginSyncException("config in plugin " + info.getName() + " is not a directory");
        }

        Files.createDirectories(destConfigDir);
        setFileAttributes(destConfigDir, CONFIG_DIR_PERMS);
        final PosixFileAttributeView destConfigDirAttributesView = Files.getFileAttributeView(
            destConfigDir.getParent(),
            PosixFileAttributeView.class
        );
        final PosixFileAttributes destConfigDirAttributes = destConfigDirAttributesView != null
            ? destConfigDirAttributesView.readAttributes()
            : null;
        if (destConfigDirAttributes != null) {
            setOwnerGroup(destConfigDir, destConfigDirAttributes);
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tmpConfigDir)) {
            for (Path srcFile : stream) {
                if (Files.isDirectory(srcFile)) {
                    throw new PluginSyncException("Directories not allowed in config dir for plugin " + info.getName());
                }

                Path destFile = destConfigDir.resolve(tmpConfigDir.relativize(srcFile));
                if (Files.exists(destFile) == false) {
                    Files.copy(srcFile, destFile);
                    setFileAttributes(destFile, CONFIG_FILES_PERMS);
                    if (destConfigDirAttributes != null) {
                        setOwnerGroup(destFile, destConfigDirAttributes);
                    }
                }
            }
        }
        IOUtils.rm(tmpConfigDir); // clean up what we just copied
    }

    private static void setOwnerGroup(final Path path, final PosixFileAttributes attributes) throws IOException {
        Objects.requireNonNull(attributes);
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        assert fileAttributeView != null;
        fileAttributeView.setOwner(attributes.owner());
        fileAttributeView.setGroup(attributes.group());
    }

    /**
     * Sets the attributes for a path iff posix attributes are supported
     */
    private static void setFileAttributes(final Path path, final Set<PosixFilePermission> permissions) throws IOException {
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (fileAttributeView != null) {
            Files.setPosixFilePermissions(path, permissions);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.rm(pathsToDeleteOnShutdown.toArray(new Path[0]));
    }
}
