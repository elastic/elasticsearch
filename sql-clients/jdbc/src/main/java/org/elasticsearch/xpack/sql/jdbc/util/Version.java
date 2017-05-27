/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

public abstract class Version {

    private static final String UNKNOWN = "Unknown";
    private static final String VER;
    private static final String HASH;
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

        Properties build = new Properties();
        try {
            build = IOUtils.propsFromString(IOUtils.asString(Version.class.getResourceAsStream("/jdbc-build.properties")));
        } catch (Exception ex) {
            // ignore if no build info was found
        }
        VER_MAJ = Integer.parseInt(build.getProperty("version.major", "1"));
        VER_MIN = Integer.parseInt(build.getProperty("version.minor", "0"));
        VER = build.getProperty("version", UNKNOWN);
        HASH = build.getProperty("hash", UNKNOWN);
        SHORT_HASH = HASH.length() > 10 ? HASH.substring(0, 10) : HASH;
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

    public static String versionHash() {
        return HASH;
    }

    public static String versionHashShort() {
        return SHORT_HASH;
    }
}