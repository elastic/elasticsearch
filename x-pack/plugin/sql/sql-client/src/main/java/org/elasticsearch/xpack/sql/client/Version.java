/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class Version {

    public static final Version CURRENT;
    public final String version;
    public final String hash;
    public final byte major;
    public final byte minor;
    public final byte revision;

    private Version(String version, String hash, byte... parts) {
        this.version = version;
        this.hash = hash;
        this.major = parts[0];
        this.minor = parts[1];
        this.revision = parts[2];
    }

    public static Version fromString(String version) {
        return new Version(version, "Unknown", from(version));
    }

    static byte[] from(String ver) {
        String[] parts = ver.split("[.-]");
        // Allow for optional snapshot and qualifier
        if (parts.length < 3 || parts.length > 5) {
            throw new IllegalArgumentException("Invalid version " + ver);
        }
        else {
            return new byte[] { Byte.parseByte(parts[0]), Byte.parseByte(parts[1]), Byte.parseByte(parts[2]) };
        }
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
        String urlStr = url.toString();

        byte maj = 0, min = 0, rev = 0;
        String ver = "Unknown";
        String hash = ver;

        if (urlStr.endsWith(".jar")) {
            try (JarInputStream jar = new JarInputStream(url.openStream())) {
                Manifest manifest = jar.getManifest();
                hash = manifest.getMainAttributes().getValue("Change");
                ver = manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Version");
                byte[] vers = from(ver);
                maj = vers[0];
                min = vers[1];
                rev = vers[2];
            } catch (Exception ex) {
                throw new IllegalArgumentException("Detected Elasticsearch JDBC jar but cannot retrieve its version", ex);
            }
        }
        CURRENT = new Version(ver, hash, maj, min, rev);
    }

    @Override
    public String toString() {
        return "v" + version + " [" + hash + "]";
    }

    public static int jdbcMajorVersion() {
        return 4;
    }

    public static int jdbcMinorVersion() {
        return 2;
    }
}
