/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.script.ClassPermission;

import javax.management.MBeanPermission;
import javax.management.MBeanServerPermission;
import javax.management.ObjectName;
import javax.security.auth.AuthPermission;
import javax.security.auth.PrivateCredentialPermission;
import javax.security.auth.kerberos.DelegationPermission;
import javax.security.auth.kerberos.ServicePermission;
import java.io.FilePermission;
import java.io.IOException;
import java.lang.reflect.ReflectPermission;
import java.net.NetPermission;
import java.net.SocketPermission;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLPermission;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.NoSuchAlgorithmException;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.SecurityPermission;
import java.security.URIParameter;
import java.security.UnresolvedPermission;
import java.security.cert.Certificate;
import java.sql.SQLPermission;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PropertyPermission;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PolicyUtil {

    // this object is checked by reference, so the value in the list does not matter
    static final List<String> ALLOW_ALL_NAMES = org.elasticsearch.core.List.of("ALLOW ALL NAMES SENTINEL");

    static class PermissionMatcher implements Predicate<Permission> {

        PermissionCollection namedPermissions;
        Map<String, List<String>> classPermissions;

        PermissionMatcher(PermissionCollection namedPermissions,
                          Map<String, List<String>> classPermissions) {
            this.namedPermissions = namedPermissions;
            this.classPermissions = classPermissions;
        }

        @Override
        public boolean test(Permission permission) {
            if (namedPermissions.implies(permission)) {
                return true;
            }
            String clazz = permission.getClass().getCanonicalName();
            String name = permission.getName();
            if (permission.getClass().equals(UnresolvedPermission.class)) {
                UnresolvedPermission up = (UnresolvedPermission) permission;
                clazz = up.getUnresolvedType();
                name = up.getUnresolvedName();
            }
            List<String> allowedNames = classPermissions.get(clazz);
            return allowedNames != null && (allowedNames == ALLOW_ALL_NAMES || allowedNames.contains(name));
        }
    }

    private static final PermissionMatcher ALLOWED_PLUGIN_PERMISSIONS;
    private static final PermissionMatcher ALLOWED_MODULE_PERMISSIONS;
    static {
        List<Permission> namedPermissions = org.elasticsearch.core.List.of(
            // TODO: remove read permission, see https://github.com/elastic/elasticsearch/issues/69464
            createFilePermission("<<ALL FILES>>", "read"),

            new ReflectPermission("suppressAccessChecks"),
            new RuntimePermission("createClassLoader"),
            new RuntimePermission("getClassLoader"),
            new RuntimePermission("setContextClassLoader"),
            new RuntimePermission("setFactory"),
            new RuntimePermission("loadLibrary.*"),
            new RuntimePermission("accessClassInPackage.*"),
            new RuntimePermission("accessDeclaredMembers"),
            new NetPermission("requestPasswordAuthentication"),
            new NetPermission("getProxySelector"),
            new NetPermission("getCookieHandler"),
            new NetPermission("getResponseCache"),
            new SocketPermission("*", "accept,connect,listen,resolve"),
            new SecurityPermission("createAccessControlContext"),
            new SecurityPermission("insertProvider"),
            new SecurityPermission("putProviderProperty.*"),
            // apache abuses the SecurityPermission class for it's own purposes
            new SecurityPermission("org.apache.*"),
            // write is needed because of HdfsPlugin
            new PropertyPermission("*", "read,write"),
            new AuthPermission("doAs"),
            new AuthPermission("doAsPrivileged"),
            new AuthPermission("getSubject"),
            new AuthPermission("getSubjectFromDomainCombiner"),
            new AuthPermission("setReadOnly"),
            new AuthPermission("modifyPrincipals"),
            new AuthPermission("modifyPublicCredentials"),
            new AuthPermission("modifyPrivateCredentials"),
            new AuthPermission("refreshCredential"),
            new AuthPermission("destroyCredential"),
            new AuthPermission("createLoginContext.*"),
            new AuthPermission("getLoginConfiguration"),
            new AuthPermission("setLoginConfiguration"),
            new AuthPermission("createLoginConfiguration.*"),
            new AuthPermission("refreshLoginConfiguration"),
            new MBeanPermission("*",  "*", ObjectName.WILDCARD,
                "addNotificationListener,getAttribute,getDomains,getMBeanInfo,getObjectInstance,instantiate,invoke," +
                "isInstanceOf,queryMBeans,queryNames,registerMBean,removeNotificationListener,setAttribute,unregisterMBean"),
            new MBeanServerPermission("*")
        );
        // While it would be ideal to represent all allowed permissions with concrete instances so that we can
        // use the builtin implies method to match them against the parsed policy, this does not work in all
        // cases for two reasons:
        // (1) Some permissions classes do not have a name argument that can represent all possible variants.
        //     For example, FilePermission has "<< ALL FILES >>" so all paths can be matched, but DelegationPermission
        //     does not have anything to represent all principals.
        // (2) Some permissions classes are in java modules that are not accessible from the classloader used by
        //     the policy parser. This results in those permissions being in UnresolvedPermission instances. Those
        //     are normally resolved at runtime when that permission is checked by SecurityManager. But there is
        //     no general purpose utility to resolve those permissions, so we must be able to match those
        //     unresolved permissions in the policy by class and name values.
        // Given the above, the below map is from permission class to the list of allowed name values. A sentinel value
        // is used to mean names are accepted. We do not use this model for all permissions because many permission
        // classes have their own meaning for some form of wildcard matching of the name, which we want to delegate
        // to those permissions if possible.
        Map<String, List<String>> classPermissions = org.elasticsearch.core.Map.of(
            URLPermission.class, ALLOW_ALL_NAMES,
            DelegationPermission.class, ALLOW_ALL_NAMES,
            ServicePermission.class, ALLOW_ALL_NAMES,
            PrivateCredentialPermission.class, ALLOW_ALL_NAMES,
            SQLPermission.class, org.elasticsearch.core.List.of("callAbort", "setNetworkTimeout"),
            ClassPermission.class, ALLOW_ALL_NAMES
        ).entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getCanonicalName(), Map.Entry::getValue));
        PermissionCollection pluginPermissionCollection = new Permissions();
        namedPermissions.forEach(pluginPermissionCollection::add);
        pluginPermissionCollection.setReadOnly();
        ALLOWED_PLUGIN_PERMISSIONS = new PermissionMatcher(pluginPermissionCollection, classPermissions);

        // Modules are allowed a few extra permissions. While we should strive to keep this list small, modules
        // are essentially part of core, so these are permissions we need for various reasons in core functionality,
        // but that we do not think plugins in general should need.
        List<Permission> modulePermissions = org.elasticsearch.core.List.of(
            createFilePermission("<<ALL FILES>>", "read,write"),
            new RuntimePermission("getFileStoreAttributes"),
            new RuntimePermission("accessUserInformation"),
            new AuthPermission("modifyPrivateCredentials")
        );
        PermissionCollection modulePermissionCollection = new Permissions();
        namedPermissions.forEach(modulePermissionCollection::add);
        modulePermissions.forEach(modulePermissionCollection::add);
        modulePermissionCollection.setReadOnly();
        ALLOWED_MODULE_PERMISSIONS = new PermissionMatcher(modulePermissionCollection, classPermissions);
    }

    @SuppressForbidden(reason = "create permission for test")
    private static FilePermission createFilePermission(String path, String actions) {
        return new FilePermission(path, actions);
    }

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
            Properties originalProps = System.getProperties();
            // allow missing while still setting values
            Set<String> unknownCodebases = new HashSet<>();
            Map<String, String> codebaseProperties = new HashMap<>();
            Properties tempProps = new Properties(originalProps) {
                @Override
                public String getProperty(String key) {
                    if (key.startsWith("codebase.")) {
                        String value = codebaseProperties.get(key);
                        if (value == null) {
                            unknownCodebases.add(key);
                        }
                        return value;
                    } else {
                        return super.getProperty(key);
                    }
                }
            };

            try {
                System.setProperties(tempProps);
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

                        Object previous = codebaseProperties.put(aliasProperty, url.toString());
                        if (previous != null) {
                            throw new IllegalStateException("codebase property already set: " + aliasProperty + " -> " + previous +
                                ", cannot set to " + url.toString());
                        }
                    }
                    Object previous = codebaseProperties.put(property, url.toString());
                    if (previous != null) {
                        throw new IllegalStateException("codebase property already set: " + property + " -> " + previous +
                                                        ", cannot set to " + url.toString());
                    }
                }
                Policy policy = Policy.getInstance("JavaPolicy", new URIParameter(policyFile.toURI()));
                if (unknownCodebases.isEmpty() == false) {
                    throw new IllegalArgumentException("Unknown codebases " + unknownCodebases + " in policy file [" + policyFile + "]" +
                        "\nAvailable codebases: " + codebaseProperties.keySet());
                }
                return policy;
            } finally {
                System.setProperties(originalProps);
            }
        } catch (NoSuchAlgorithmException | URISyntaxException e) {
            throw new IllegalArgumentException("unable to parse policy file `" + policyFile + "`", e);
        }
    }

    // pakcage private for tests
    static PluginPolicyInfo readPolicyInfo(Path pluginRoot) throws IOException {
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
        // also add spi jars
        // TODO: move this to a shared function, or fix plugin layout to have jar files in lib directory
        Path spiDir = pluginRoot.resolve("spi");
        if (Files.exists(spiDir)) {
            try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(spiDir, "*.jar")) {
                for (Path jar : jarStream) {
                    URL url = jar.toRealPath().toUri().toURL();
                    if (jars.add(url) == false) {
                        throw new IllegalStateException("duplicate module/plugin: " + url);
                    }
                }
            }
        }

        // parse the plugin's policy file into a set of permissions
        Policy policy = readPolicy(policyFile.toUri().toURL(), getCodebaseJarMap(jars));

        return new PluginPolicyInfo(policyFile, jars, policy);
    }

    private static void validatePolicyPermissionsForJar(String type, Path file, URL jar, Policy policy,
                                                        PermissionMatcher allowedPermissions, Path tmpDir) throws IOException {
        Set<Permission> jarPermissions = getPolicyPermissions(jar, policy, tmpDir);
        for (Permission permission : jarPermissions) {
            if (allowedPermissions.test(permission) == false) {
                String scope = jar == null ? " in global grant" : " for jar " + jar;
                throw new IllegalArgumentException(type + " policy [" + file + "] contains illegal permission " + permission + scope);
            }
        }
    }

    private static void validatePolicyPermissions(String type, PluginPolicyInfo info, PermissionMatcher allowedPermissions,
                                                  Path tmpDir) throws IOException {
        if (info == null) {
            return;
        }
        validatePolicyPermissionsForJar(type, info.file, null, info.policy, allowedPermissions, tmpDir);
        for (URL jar : info.jars) {
            validatePolicyPermissionsForJar(type, info.file, jar, info.policy, allowedPermissions, tmpDir);
        }
    }

    /**
     * Return info about the security policy for a plugin.
     */
    public static PluginPolicyInfo getPluginPolicyInfo(Path pluginRoot, Path tmpDir) throws IOException {
        PluginPolicyInfo info = readPolicyInfo(pluginRoot);
        validatePolicyPermissions("plugin", info, ALLOWED_PLUGIN_PERMISSIONS, tmpDir);
        return info;
    }

    /**
     * Return info about the security policy for a module.
     */
    public static PluginPolicyInfo getModulePolicyInfo(Path moduleRoot, Path tmpDir) throws IOException {
        PluginPolicyInfo info = readPolicyInfo(moduleRoot);
        validatePolicyPermissions("module", info, ALLOWED_MODULE_PERMISSIONS, tmpDir);
        return info;
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
