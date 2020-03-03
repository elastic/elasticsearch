/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class Version extends org.elasticsearch.xpack.sql.proto.Version {

    public static final Version CURRENT;
    public final String hash;

    private Version(String version, String hash, byte... parts) {
        super(version, parts);
        this.hash = hash;
    }

    public static Version fromString(String version) {
        return new Version(version, "Unknown", from(version));
    }


    static {
        // check classpath
        String target = Version.class.getName().replace(".", "/").concat(".class");
        Enumeration<URL> res;

        try {
            res = Version.class.getClassLoader().getResources(target);
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
                        "Multiple Elasticsearch JDBC versions detected in the classpath; please use only one\n");
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
        // Since version info is not critical, the parsing is lenient
        URL url = Version.class.getProtectionDomain().getCodeSource().getLocation();
        CURRENT = extractVersion(url);
    }

    static Version extractVersion(URL url) {
        String urlStr = url.toString();
        byte maj = 0, min = 0, rev = 0;
        String ver = "Unknown";
        String hash = ver;
        
        if (urlStr.endsWith(".jar") || urlStr.endsWith(".jar!/")) {
            try {
                URLConnection conn = url.openConnection();
                conn.setUseCaches(false);
                
                try (JarInputStream jar = new JarInputStream(conn.getInputStream())) {
                    Manifest manifest = jar.getManifest();
                    hash = manifest.getMainAttributes().getValue("Change");
                    ver = manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Version");
                    byte[] vers = from(ver);
                    maj = vers[0];
                    min = vers[1];
                    rev = vers[2];
                }
            } catch (Exception ex) {
                throw new IllegalArgumentException("Detected Elasticsearch JDBC jar but cannot retrieve its version", ex);
            }
        }
        return new Version(ver, hash, maj, min, rev);
    }

    public String versionString() {
        return "v" + version + " [" + hash + "]";
    }

    // This function helps ensure that a client won't attempt to communicate to a server with less features than its own. Since this check
    // is part of the client's start-up check that might not involve an actual SQL API request, the client has to do a bare version check
    // as well.
    public static boolean isServerCompatible(Version server) {
        // Starting with this version, the compatibility logic moved from the client to the server. Only relevant for 7.x releases.
        return server.compareTo(Version.V_7_7_0) >= 0
            // Relevant for 8+ releases. Patches bring no features, so they are excluded from the test (which means that a "newer" client
            // could actually communicate with an "older" client, within the same minor)
            && server.compareToMajorMinor(CURRENT) >= 0;
    }

    public static int jdbcMajorVersion() {
        return 4;
    }

    public static int jdbcMinorVersion() {
        return 2;
    }
}
