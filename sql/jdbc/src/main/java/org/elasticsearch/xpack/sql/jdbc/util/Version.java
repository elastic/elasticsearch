/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

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

    private static final int VER_MAJ, VER_MIN;


    static {
        // check classpath
        String target = Version.class.getName().replace(".", "/").concat(".class");
        Enumeration<URL> res = null;

        try {
            res = Version.class.getClassLoader().getResources(target);
        } catch (IOException ex) {
            //            LogFactory.getLog(Version.class)
            //                    .warn("Cannot detect Elasticsearch JDBC driver jar; it typically indicates a deployment issue...");
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
                        "Multiple Elasticsearch JDBC driver versions detected in the classpath; please use only one\n");
                for (String s : normalized) {
                    if (s.contains("jar:")) {
                        foundJars++;
                        sb.append(s.replace("!/" + target, ""));
                        sb.append("\n");
                    }
                }
                if (foundJars > 1) {
//                    LogFactory.getLog(Version.class).fatal(sb);
                    throw new Error(sb.toString());
                }
            }
        }

        // This is similar to how Elasticsearch's Build class digs up its build information.
        URL url = Version.class.getProtectionDomain().getCodeSource().getLocation();
        String urlStr = url.toString();
        if (urlStr.startsWith("file:/") && (urlStr.endsWith(".jar") || urlStr.endsWith("-SNAPSHOT.jar"))) {
            try (JarInputStream jar = new JarInputStream(url.openStream())) {
                Manifest manifest = jar.getManifest();
                VER = manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Version");
                int sep = VER.indexOf('.');
                VER_MAJ = Integer.parseInt(VER.substring(0, sep - 1));
                VER_MIN = Integer.parseInt(VER.substring(sep, VER.indexOf(sep, '.') - 1));
                SHORT_HASH = manifest.getMainAttributes().getValue("Change");
            } catch (IOException e) {
                throw new RuntimeException("error finding version of driver", e);
            }
        } else {
            VER_MAJ = 0;
            VER_MIN = 0;
            VER = "Unknown";
            SHORT_HASH = "Unknown";
        }
    }

    public static int versionMajor() {
        return VER_MAJ;
    }

    public static int versionMinor() {
        return VER_MIN;
    }

    public static String version() {
        return "v" + versionNumber() + " [" + versionHashShort() + "]";
    }

    public static String versionNumber() {
        return VER;
    }

    public static String versionHashShort() {
        return SHORT_HASH;
    }
}