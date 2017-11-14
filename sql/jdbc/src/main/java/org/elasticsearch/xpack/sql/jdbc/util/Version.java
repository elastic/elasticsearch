/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import org.elasticsearch.xpack.sql.client.shared.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public abstract class Version {
    private static final String VER;
    private static final String SHORT_HASH;

    private static final int VER_MAJ, VER_MIN, VER_REV;

    static int[] from(String ver) {
        String[] parts = ver.split("[.-]");
        if (parts.length == 3 || parts.length == 4) {
            return new int[] { Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]) };
        }
        else {
            throw new Error("Detected Elasticsearch SQL JDBC driver but found invalid version " + ver);
        }
    }

    static {
        // check classpath
        String target = Version.class.getName().replace(".", "/").concat(".class");
        Enumeration<URL> res = null;

        try {
            res = Version.class.getClassLoader().getResources(target);
        } catch (IOException ex) {
            throw new Error("Cannot detect Elasticsearch SQL JDBC driver jar; it typically indicates a deployment issue...");
        }

        if (res != null) {
            List<URL> urls = Collections.list(res);
            Set<String> normalized = new LinkedHashSet<String>();

            for (URL url : urls) {
                normalized.add(StringUtils.normalize(url.toString()));
            }

            int foundJars = 0;
            if (normalized.size() > 1) {
                StringBuilder sb = new StringBuilder(
                        "Multiple Elasticsearch SQL JDBC driver versions detected in the classpath; please use only one\n");
                for (String s : normalized) {
                    if (s.contains("jar:")) {
                        foundJars++;
                        sb.append(s.replace("!/" + target, ""));
                        sb.append("\n");
                    }
                }
                if (foundJars > 1) {
                    throw new Error(sb.toString());
                }
            }
        }

        // This is similar to how Elasticsearch's Build class digs up its build information.
        // Since version info is not critical, the parsing is lenient
        URL url = Version.class.getProtectionDomain().getCodeSource().getLocation();
        String urlStr = url.toString();

        int maj = 0, min = 0, rev = 0;
        String ver = "Unknown";
        String hash = ver;

        if (urlStr.startsWith("file:/") && (urlStr.endsWith(".jar") || urlStr.endsWith("-SNAPSHOT.jar"))) {
            try (JarInputStream jar = new JarInputStream(url.openStream())) {
                Manifest manifest = jar.getManifest();
                hash = manifest.getMainAttributes().getValue("Change");
                int[] vers = from(manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Version"));
                maj = vers[0];
                min = vers[1];
                rev = vers[2];
            } catch (Exception ex) {
                throw new Error("Detected Elasticsearch SQL JDBC driver but cannot retrieve its version", ex);
            }
        }
        VER_MAJ = maj;
        VER_MIN = min;
        VER_REV = rev;
        VER = ver;
        SHORT_HASH = hash;
    }

    public static int versionMajor() {
        return VER_MAJ;
    }

    public static int versionMinor() {
        return VER_MIN;
    }

    public static int versionRevision() {
        return VER_REV;
    }

    public static String version() {
        return "v" + versionNumber() + " [" + versionHash() + "]";
    }

    public static String versionNumber() {
        return VER;
    }

    public static String versionHash() {
        return SHORT_HASH;
    }

    public static int jdbcMajorVersion() {
        return 4;
    }

    public static int jdbcMinorVersion() {
        return 2;
    }
}