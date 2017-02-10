/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.AccessController;
import java.security.URIParameter;
import java.security.NoSuchAlgorithmException;

final class XPackExtensionSecurity {
    private XPackExtensionSecurity() {}

    /**
     * Initializes the XPackExtensionPolicy
     * Can only happen once!
     *
     * @param extsDirectory the directory where the extensions are installed
     */
    static void configure(Path extsDirectory) throws IOException {
        Map<String, Policy> map = getExtensionsPermissions(extsDirectory);
        if (map.size() > 0) {
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                Policy newPolicy = new XPackExtensionPolicy(Policy.getPolicy(), map);
                Policy.setPolicy(newPolicy);
                return null;
            });
        }
    }

    /**
     * Sets properties (codebase URLs) for policy files.
     * we look for matching extensions and set URLs to fit
     */
    @SuppressForbidden(reason = "proper use of URL")
    static Map<String, Policy> getExtensionsPermissions(Path extsDirectory) throws IOException {
        Map<String, Policy> map = new HashMap<>();
        // collect up lists of extensions
        List<Path> extensionPaths = new ArrayList<>();
        if (Files.exists(extsDirectory)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(extsDirectory)) {
                for (Path extension : stream) {
                    extensionPaths.add(extension);
                }
            }
        }
        // now process each one
        for (Path extension : extensionPaths) {
            Path policyFile = extension.resolve(XPackExtensionInfo.XPACK_EXTENSION_POLICY);
            if (Files.exists(policyFile)) {
                // first get a list of URLs for the extension's jars:
                // we resolve symlinks so map is keyed on the normalize codebase name
                List<URL> codebases = new ArrayList<>();
                try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(extension, "*.jar")) {
                    for (Path jar : jarStream) {
                        codebases.add(jar.toRealPath().toUri().toURL());
                    }
                }

                // parse the extension's policy file into a set of permissions
                Policy policy = readPolicy(policyFile.toUri().toURL(), codebases.toArray(new URL[codebases.size()]));

                // consult this policy for each of the extension's jars:
                for (URL url : codebases) {
                    if (map.put(url.getFile(), policy) != null) {
                        // just be paranoid ok?
                        throw new IllegalStateException("per-extension permissions already granted for jar file: " + url);
                    }
                }
            }
        }

        return Collections.unmodifiableMap(map);
    }

    /**
     * Reads and returns the specified {@code policyFile}.
     * <p>
     * Resources (e.g. jar files and directories) listed in {@code codebases} location
     * will be provided to the policy file via a system property of the short name:
     * e.g. <code>${codebase.joda-convert-1.2.jar}</code> would map to full URL.
     */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    static Policy readPolicy(URL policyFile, URL codebases[]) throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            try {
                // set codebase properties
                for (URL url : codebases) {
                    String shortName = PathUtils.get(url.toURI()).getFileName().toString();

                    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        System.setProperty("codebase." + shortName, url.toString());
                        return null;
                    });
                }
                URIParameter uri = new URIParameter(policyFile.toURI());
                return AccessController.doPrivileged((PrivilegedAction<Policy>) () -> {
                    try {
                        return Policy.getInstance("JavaPolicy", uri);
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }
                });
            } finally {
                // clear codebase properties
                for (URL url : codebases) {
                    String shortName = PathUtils.get(url.toURI()).getFileName().toString();
                    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        System.clearProperty("codebase." + shortName);
                        return null;
                    });
                }
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("unable to parse policy file `" + policyFile + "`", e);
        }
    }
}