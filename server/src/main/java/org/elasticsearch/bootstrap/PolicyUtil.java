/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.plugins.PluginInfo;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.NoSuchAlgorithmException;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.URIParameter;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PolicyUtil {
    /**
     * Return a map from codebase name to codebase url of jar codebases used by ES core.
     */
    @SuppressForbidden(reason = "find URL path")
    public static Map<String, URL> getCodebaseJarMap(Set<URL> urls) {
        Map<String, URL> codebases = new LinkedHashMap<>(); // maintain order
        for (URL url : urls) {
            try {
                String fileName = PathUtils.get(url.toURI()).getFileName().toString();
                if (fileName.endsWith(".jar") == false) {
                    // tests :(
                    continue;
                }
                codebases.put(fileName, url);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return codebases;
    }

    /**
     * Reads and returns the specified {@code policyFile}.
     * <p>
     * Jar files listed in {@code codebases} location will be provided to the policy file via
     * a system property of the short name: e.g. <code>${codebase.joda-convert-1.2.jar}</code>
     * would map to full URL.
     */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    public static Policy readPolicy(URL policyFile, Map<String, URL> codebases) {
        try {
            List<String> propertiesSet = new ArrayList<>();
            try {
                // set codebase properties
                for (Map.Entry<String,URL> codebase : codebases.entrySet()) {
                    String name = codebase.getKey();
                    URL url = codebase.getValue();

                    // We attempt to use a versionless identifier for each codebase. This assumes a specific version
                    // format in the jar filename. While we cannot ensure all jars in all plugins use this format, nonconformity
                    // only means policy grants would need to include the entire jar filename as they always have before.
                    String property = "codebase." + name;
                    String aliasProperty = "codebase." + name.replaceFirst("-\\d+\\.\\d+.*\\.jar", "");
                    if (aliasProperty.equals(property) == false) {
                        propertiesSet.add(aliasProperty);
                        String previous = System.setProperty(aliasProperty, url.toString());
                        if (previous != null) {
                            throw new IllegalStateException("codebase property already set: " + aliasProperty + " -> " + previous +
                                                            ", cannot set to " + url.toString());
                        }
                    }
                    propertiesSet.add(property);
                    String previous = System.setProperty(property, url.toString());
                    if (previous != null) {
                        throw new IllegalStateException("codebase property already set: " + property + " -> " + previous +
                                                        ", cannot set to " + url.toString());
                    }
                }
                return Policy.getInstance("JavaPolicy", new URIParameter(policyFile.toURI()));
            } finally {
                // clear codebase properties
                for (String property : propertiesSet) {
                    System.clearProperty(property);
                }
            }
        } catch (NoSuchAlgorithmException | URISyntaxException e) {
            throw new IllegalArgumentException("unable to parse policy file `" + policyFile + "`", e);
        }
    }

    /**
     * Return info about the security policy for a plugin.
     */
    public static PluginPolicyInfo getPluginPolicyInfo(Path pluginRoot) throws IOException {
        Path policyFile = pluginRoot.resolve(PluginInfo.ES_PLUGIN_POLICY);
        if (Files.exists(policyFile) == false) {
            return null;
        }

        // first get a list of URLs for the plugins' jars:
        // we resolve symlinks so map is keyed on the normalize codebase name
        Set<URL> jars = new LinkedHashSet<>(); // order is already lost, but some filesystems have it
        try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(pluginRoot, "*.jar")) {
            for (Path jar : jarStream) {
                URL url = jar.toRealPath().toUri().toURL();
                if (jars.add(url) == false) {
                    throw new IllegalStateException("duplicate module/plugin: " + url);
                }
            }
        }

        // parse the plugin's policy file into a set of permissions
        Policy policy = readPolicy(policyFile.toUri().toURL(), getCodebaseJarMap(jars));

        return new PluginPolicyInfo(jars, policy);
    }

    /**
     * Return permissions for a policy that apply to a jar.
     *
     * @param url The url of a jar to find permissions for, or {@code null} for global permissions.
     */
    public static Set<Permission> getPolicyPermissions(URL url, Policy policy, Path tmpDir) throws IOException {
        // create a zero byte file for "comparison"
        // this is necessary because the default policy impl automatically grants two permissions:
        // 1. permission to exitVM (which we ignore)
        // 2. read permission to the code itself (e.g. jar file of the code)

        Path emptyPolicyFile = Files.createTempFile(tmpDir, "empty", "tmp");
        final Policy emptyPolicy;
        try {
            emptyPolicy = Policy.getInstance("JavaPolicy", new URIParameter(emptyPolicyFile.toUri()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        IOUtils.rm(emptyPolicyFile);

        final ProtectionDomain protectionDomain;
        if (url == null) {
            // global, use PolicyUtil since it is part of core ES
            protectionDomain = PolicyUtil.class.getProtectionDomain();
        } else {
            // we may not have the url loaded, so create a fake protection domain
            protectionDomain = new ProtectionDomain(new CodeSource(url, (Certificate[]) null), null);
        }

        PermissionCollection permissions = policy.getPermissions(protectionDomain);
        // this method is supported with the specific implementation we use, but just check for safety.
        if (permissions == Policy.UNSUPPORTED_EMPTY_COLLECTION) {
            throw new UnsupportedOperationException("JavaPolicy implementation does not support retrieving permissions");
        }


        Set<Permission> actualPermissions = new HashSet<>();
        for (Permission permission : Collections.list(permissions.elements())) {
            if (emptyPolicy.implies(protectionDomain, permission) == false) {
                actualPermissions.add(permission);
            }
        }

        return actualPermissions;
    }
}
