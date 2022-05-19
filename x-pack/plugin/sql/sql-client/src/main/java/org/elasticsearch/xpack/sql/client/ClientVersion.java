/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * Clients-specific version utility class.
 * <p>
 *     The class provides the SQL clients the version identifying the release they're are part of. The version is read from the
 *     encompassing JAR file (Elasticsearch-specific attribute in the manifest).
 *     The class is also a provider for the implemented JDBC standard.
 * </p>
 */
public class ClientVersion {

    public static final SqlVersion CURRENT;

    static {
        // check classpath
        String target = ClientVersion.class.getName().replace(".", "/").concat(".class");
        Enumeration<URL> res;

        try {
            res = ClientVersion.class.getClassLoader().getResources(target);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Cannot detect Elasticsearch JDBC jar; it typically indicates a deployment issue...");
        }

        if (res != null) {
            List<URL> urls = Collections.list(res);
            Set<String> normalized = new LinkedHashSet<>();

            for (URL url : urls) {
                normalized.add(StringUtils.normalize(url.toString()));
            }

            int foundJars = 0;
            if (normalized.size() > 1) {
                StringBuilder sb = new StringBuilder(
                    "Multiple Elasticsearch JDBC versions detected in the classpath; please use only one\n"
                );
                for (String s : normalized) {
                    if (s.contains("jar:")) {
                        foundJars++;
                        sb.append(s.replace("!/" + target, ""));
                        sb.append("\n");
                    }
                }
                if (foundJars > 1) {
                    throw new IllegalArgumentException(sb.toString());
                }
            }
        }

        // This is similar to how Elasticsearch's Build class digs up its build information.
        URL url = SqlVersion.class.getProtectionDomain().getCodeSource().getLocation();
        CURRENT = extractVersion(url);
    }

    // There are three main types of provided URLs:
    // (1) a file URL: file:<path><FS separator><driver name>.jar
    // (2) jar file URL pointing to a JAR file: jar:<sub-url><separator><driver name>.jar!/
    // (3) jar file URL pointing to a JAR file entry (likely a fat JAR, but other types are possible): jar:<sub-url>!/driver name>.jar!/
    @SuppressForbidden(reason = "java.util.jar.JarFile must be explicitly closed on Windows")
    static Manifest getManifest(URL url) throws IOException {
        String urlStr = url.toString();
        if (urlStr.endsWith(".jar") || urlStr.endsWith(".jar!/")) {
            URLConnection conn = url.openConnection();
            // avoid file locking
            conn.setUseCaches(false);
            // For a jar protocol, the implementing java.base/sun.net.www.protocol.jar.JarUrlConnection#getInputStream() will only
            // return a stream (vs. throw an IOException) if the JAR file URL points to a JAR file entry and not a JAR file.
            if (url.getProtocol().equals("jar")) {
                JarURLConnection jarConn = (JarURLConnection) conn;
                if (jarConn.getEntryName() == null) { // the URL points to a JAR file
                    try (JarFile jar = jarConn.getJarFile()) { // prevent locked file errors in Windows.
                        return jar.getManifest(); // in case of a fat JAR, this would return the outermost JAR's manifest
                    }
                }
            }
            try (JarInputStream jar = new JarInputStream(conn.getInputStream())) {
                return jar.getManifest();
            }
        }
        return null;
    }

    static SqlVersion extractVersion(URL url) {
        Manifest manifest = null;
        try {
            manifest = getManifest(url);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Detected an Elasticsearch JDBC jar but cannot retrieve its version", ex);
        }
        String version = manifest != null ? manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Version") : null;
        return version != null ? SqlVersion.fromString(version) : new SqlVersion(0, 0, 0);
    }

    // This function helps ensure that a client won't attempt to communicate to a server with less features than its own. Since this check
    // is part of the client's start-up check that might not involve an actual SQL API request, the client has to do a bare version check
    // as well.
    public static boolean isServerCompatible(SqlVersion server) {
        // Starting with this version, the compatibility logic moved from the client to the server.
        return SqlVersion.hasVersionCompatibility(server);
    }

    public static int jdbcMajorVersion() {
        return 4;
    }

    public static int jdbcMinorVersion() {
        return 2;
    }
}
