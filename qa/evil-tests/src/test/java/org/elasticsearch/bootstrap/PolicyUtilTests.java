/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Policy;
import java.security.URIParameter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsMapContaining.hasKey;

public class PolicyUtilTests extends ESTestCase {

    @Before
    public void assumeSecurityManagerDisabled() {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
    }

    URL makeUrl(String s) {
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    Path makeDummyPlugin(String policy, String... files) throws IOException {
        Path plugin = createTempDir();
        Files.copy(this.getDataPath(policy), plugin.resolve(PluginInfo.ES_PLUGIN_POLICY));
        for (String file : files) {
            Files.createFile(plugin.resolve(file));
        }
        return plugin;
    }

    @SuppressForbidden(reason = "set for test")
    void setProperty(String key, String value) {
        System.setProperty(key, value);
    }

    @SuppressForbidden(reason = "cleanup test")
    void clearProperty(String key) {
        System.clearProperty(key);
    }

    public void testCodebaseJarMap() throws Exception {
        Set<URL> urls = new LinkedHashSet<>(List.of(
            makeUrl("file:///foo.jar"),
            makeUrl("file:///bar.txt"),
            makeUrl("file:///a/bar.jar")
        ));

        Map<String, URL> jarMap = PolicyUtil.getCodebaseJarMap(urls);
        assertThat(jarMap, hasKey("foo.jar"));
        assertThat(jarMap, hasKey("bar.jar"));
        // only jars are grabbed
        assertThat(jarMap, not(hasKey("bar.txt")));

        // order matters
        assertThat(jarMap.keySet(), contains("foo.jar", "bar.jar"));
    }

    public void testPluginPolicyInfoEmpty() throws Exception {
        assertThat(PolicyUtil.readPolicyInfo(createTempDir()), is(nullValue()));
    }

    public void testPluginPolicyInfoNoJars() throws Exception {
        Path noJarsPlugin = makeDummyPlugin("dummy.policy");
        PluginPolicyInfo info = PolicyUtil.readPolicyInfo(noJarsPlugin);
        assertThat(info.policy, is(not(nullValue())));
        assertThat(info.jars, emptyIterable());
    }

    public void testPluginPolicyInfo() throws Exception {
        Path plugin = makeDummyPlugin("dummy.policy",
            "foo.jar", "foo.txt", "bar.jar");
        PluginPolicyInfo info = PolicyUtil.readPolicyInfo(plugin);
        assertThat(info.policy, is(not(nullValue())));
        assertThat(info.jars, containsInAnyOrder(
            plugin.resolve("foo.jar").toUri().toURL(),
            plugin.resolve("bar.jar").toUri().toURL()));
    }

    public void testPolicyMissingCodebaseProperty() throws Exception {
        Path plugin = makeDummyPlugin("missing-codebase.policy", "foo.jar");
        URL policyFile = plugin.resolve(PluginInfo.ES_PLUGIN_POLICY).toUri().toURL();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PolicyUtil.readPolicy(policyFile, Map.of()));
        assertThat(e.getMessage(), containsString("Unknown codebases [codebase.doesnotexist] in policy file"));
    }

    public void testPolicyPermissions() throws Exception {
        Path plugin = makeDummyPlugin("global-and-jar.policy", "foo.jar", "bar.jar");
        Path tmpDir = createTempDir();
        try {
            URL jarUrl = plugin.resolve("foo.jar").toUri().toURL();
            setProperty("jarUrl", jarUrl.toString());
            URL policyFile = plugin.resolve(PluginInfo.ES_PLUGIN_POLICY).toUri().toURL();
            Policy policy = Policy.getInstance("JavaPolicy", new URIParameter(policyFile.toURI()));

            Set<Permission> globalPermissions = PolicyUtil.getPolicyPermissions(null, policy, tmpDir);
            assertThat(globalPermissions, contains(new RuntimePermission("queuePrintJob")));

            Set<Permission> jarPermissions = PolicyUtil.getPolicyPermissions(jarUrl, policy, tmpDir);
            assertThat(jarPermissions,
                containsInAnyOrder(new RuntimePermission("getClassLoader"), new RuntimePermission("queuePrintJob")));
        } finally {
            clearProperty("jarUrl");
        }
    }

    private Path makeSinglePermissionPlugin(String jarUrl, String clazz, String name, String actions) throws IOException {
        Path plugin = createTempDir();
        StringBuilder policyString = new StringBuilder("grant");
        if (jarUrl != null) {
            Path jar = plugin.resolve(jarUrl);
            Files.createFile(jar);
            policyString.append(" codeBase \"" + jar.toUri().toURL().toString() + "\"");
        }
        policyString.append(" {\n  permission ");
        policyString.append(clazz);
        // wildcard
        policyString.append(" \"" + name + "\"");
        if (actions != null) {
            policyString.append(", \"" + actions + "\"");
        }
        policyString.append(";\n};");

        logger.info(policyString.toString());
        Files.writeString(plugin.resolve(PluginInfo.ES_PLUGIN_POLICY), policyString.toString());

        return plugin;
    }

    interface PolicyParser {
        PluginPolicyInfo parse(Path pluginRoot, Path tmpDir) throws IOException;
    }

    void assertAllowedPermission(String clazz, String name, String actions, Path tmpDir, PolicyParser parser) throws Exception {
        // global policy
        Path plugin = makeSinglePermissionPlugin(null, clazz, name, actions);
        assertNotNull(parser.parse(plugin, tmpDir)); // no error

        // specific jar policy
        plugin = makeSinglePermissionPlugin("foobar.jar", clazz, name, actions);
        assertNotNull(parser.parse(plugin, tmpDir)); // no error
    }

    void assertAllowedPermissions(List<String> allowedPermissions, PolicyParser parser) throws Exception {
        Path tmpDir = createTempDir();
        for (String rawPermission : allowedPermissions) {
            String[] elements = rawPermission.split(" ");
            assert elements.length <= 3;
            assert elements.length >= 2;

            assertAllowedPermission(elements[0], elements[1], elements.length == 3 ? elements[2] : null, tmpDir, parser);
        }
    }

    void assertIllegalPermission(String clazz, String name, String actions, Path tmpDir, PolicyParser parser) throws Exception {
        // global policy
        final Path globalPlugin = makeSinglePermissionPlugin(null, clazz, name, actions);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            "Permission (" + clazz + " " + name + (actions == null ? "" : (" " + actions)) + ") should be illegal",
            () -> parser.parse(globalPlugin, tmpDir)); // no error
        assertThat(e.getMessage(), containsString("contains illegal permission"));
        assertThat(e.getMessage(), containsString("in global grant"));

        // specific jar policy
        final Path jarPlugin = makeSinglePermissionPlugin("foobar.jar", clazz, name, actions);
        e = expectThrows(IllegalArgumentException.class, () -> parser.parse(jarPlugin, tmpDir)); // no error
        assertThat(e.getMessage(), containsString("contains illegal permission"));
        assertThat(e.getMessage(), containsString("for jar"));
    }

    void assertIllegalPermissions(List<String> illegalPermissions, PolicyParser parser) throws Exception {
        Path tmpDir = createTempDir();
        for (String rawPermission : illegalPermissions) {
            String[] elements = rawPermission.split(" ");
            assert elements.length <= 3;
            assert elements.length >= 2;

            assertIllegalPermission(elements[0], elements[1], elements.length == 3 ? elements[2] : null, tmpDir, parser);
        }
    }

    static final List<String> PLUGIN_TEST_PERMISSIONS = List.of(
        // TODO: move this back to module test permissions, see https://github.com/elastic/elasticsearch/issues/69464
        "java.io.FilePermission /foo/bar read",

        "java.lang.reflect.ReflectPermission suppressAccessChecks",
        "java.lang.RuntimePermission getClassLoader",
        "java.lang.RuntimePermission setContextClassLoader",
        "java.lang.RuntimePermission setFactory",
        "java.lang.RuntimePermission loadLibrary.*",
        "java.lang.RuntimePermission accessClassInPackage.*",
        "java.lang.RuntimePermission accessDeclaredMembers",
        "java.net.NetPermission requestPasswordAuthentication",
        "java.net.NetPermission getProxySelector",
        "java.net.NetPermission getCookieHandler",
        "java.net.NetPermission getResponseCache",
        "java.net.SocketPermission * accept,connect,listen,resolve",
        "java.net.SocketPermission www.elastic.co accept,connect,listen,resolve",
        "java.net.URLPermission https://elastic.co",
        "java.net.URLPermission http://elastic.co",
        "java.security.SecurityPermission createAccessControlContext",
        "java.security.SecurityPermission insertProvider",
        "java.security.SecurityPermission putProviderProperty.*",
        "java.security.SecurityPermission putProviderProperty.foo",
        "java.sql.SQLPermission callAbort",
        "java.sql.SQLPermission setNetworkTimeout",
        "java.util.PropertyPermission * read",
        "java.util.PropertyPermission someProperty read",
        "java.util.PropertyPermission * write",
        "java.util.PropertyPermission foo.bar write",
        "javax.management.MBeanPermission * addNotificationListener",
        "javax.management.MBeanPermission * getAttribute",
        "javax.management.MBeanPermission * getDomains",
        "javax.management.MBeanPermission * getMBeanInfo",
        "javax.management.MBeanPermission * getObjectInstance",
        "javax.management.MBeanPermission * instantiate",
        "javax.management.MBeanPermission * invoke",
        "javax.management.MBeanPermission * isInstanceOf",
        "javax.management.MBeanPermission * queryMBeans",
        "javax.management.MBeanPermission * queryNames",
        "javax.management.MBeanPermission * registerMBean",
        "javax.management.MBeanPermission * removeNotificationListener",
        "javax.management.MBeanPermission * setAttribute",
        "javax.management.MBeanPermission * unregisterMBean",
        "javax.management.MBeanServerPermission *",
        "javax.security.auth.AuthPermission doAs",
        "javax.security.auth.AuthPermission doAsPrivileged",
        "javax.security.auth.AuthPermission getSubject",
        "javax.security.auth.AuthPermission getSubjectFromDomainCombiner",
        "javax.security.auth.AuthPermission setReadOnly",
        "javax.security.auth.AuthPermission modifyPrincipals",
        "javax.security.auth.AuthPermission modifyPublicCredentials",
        "javax.security.auth.AuthPermission modifyPrivateCredentials",
        "javax.security.auth.AuthPermission refreshCredential",
        "javax.security.auth.AuthPermission destroyCredential",
        "javax.security.auth.AuthPermission createLoginContext.*",
        "javax.security.auth.AuthPermission getLoginConfiguration",
        "javax.security.auth.AuthPermission setLoginConfiguration",
        "javax.security.auth.AuthPermission createLoginConfiguration.*",
        "javax.security.auth.AuthPermission refreshLoginConfiguration",
        "javax.security.auth.kerberos.DelegationPermission host/www.elastic.co@ELASTIC.CO krbtgt/ELASTIC.CO@ELASTIC.CO",
        "javax.security.auth.kerberos.ServicePermission host/www.elastic.co@ELASTIC.CO accept"
    );

    public void testPluginPolicyAllowedPermissions() throws Exception {
        assertAllowedPermissions(PLUGIN_TEST_PERMISSIONS, PolicyUtil::getPluginPolicyInfo);
        assertIllegalPermissions(MODULE_TEST_PERMISSIONS, PolicyUtil::getPluginPolicyInfo);
    }

    public void testPrivateCredentialPermissionAllowed() throws Exception {
        // the test permission list relies on name values not containing spaces, so this
        // exists to also check PrivateCredentialPermission which requires a space in the name
        String clazz = "javax.security.auth.PrivateCredentialPermission";
        String name = "com.sun.PrivateCredential com.sun.Principal \\\"duke\\\"";

        assertAllowedPermission(clazz, name, "read", createTempDir(), PolicyUtil::getPluginPolicyInfo);
    }

    static final List<String> MODULE_TEST_PERMISSIONS = List.of(
        "java.io.FilePermission /foo/bar write",
        "java.lang.RuntimePermission createClassLoader",
        "java.lang.RuntimePermission getFileStoreAttributes",
        "java.lang.RuntimePermission accessUserInformation"
    );

    public void testModulePolicyAllowedPermissions() throws Exception {
        assertAllowedPermissions(MODULE_TEST_PERMISSIONS, PolicyUtil::getModulePolicyInfo);
    }

    static final List<String> ILLEGAL_TEST_PERMISSIONS = List.of(
        "java.awt.AWTPermission *",
        "java.io.FilePermission /foo/bar execute",
        "java.io.FilePermission /foo/bar delete",
        "java.io.FilePermission /foo/bar readLink",
        "java.io.SerializablePermission enableSubclassImplementation",
        "java.io.SerializablePermission enableSubstitution",
        "java.lang.management.ManagementPermission control",
        "java.lang.management.ManagementPermission monitor",
        "java.lang.reflect.ReflectPermission newProxyInPackage.*",
        "java.lang.RuntimePermission enableContextClassLoaderOverride",
        "java.lang.RuntimePermission closeClassLoader",
        "java.lang.RuntimePermission setSecurityManager",
        "java.lang.RuntimePermission createSecurityManager",
        "java.lang.RuntimePermission getenv.*",
        "java.lang.RuntimePermission getenv.FOOBAR",
        "java.lang.RuntimePermission shutdownHooks",
        "java.lang.RuntimePermission setIO",
        "java.lang.RuntimePermission modifyThread",
        "java.lang.RuntimePermission stopThread",
        "java.lang.RuntimePermission modifyThreadGroup",
        "java.lang.RuntimePermission getProtectionDomain",
        "java.lang.RuntimePermission readFileDescriptor",
        "java.lang.RuntimePermission writeFileDescriptor",
        "java.lang.RuntimePermission defineClassInPackage.*",
        "java.lang.RuntimePermission defineClassInPackage.foobar",
        "java.lang.RuntimePermission queuePrintJob",
        "java.lang.RuntimePermission getStackTrace",
        "java.lang.RuntimePermission setDefaultUncaughtExceptionHandler",
        "java.lang.RuntimePermission preferences",
        "java.lang.RuntimePermission usePolicy",
        // blanket runtime permission not allowed
        "java.lang.RuntimePermission *",
        "java.net.NetPermission setDefaultAuthenticator",
        "java.net.NetPermission specifyStreamHandler",
        "java.net.NetPermission setProxySelector",
        "java.net.NetPermission setCookieHandler",
        "java.net.NetPermission setResponseCache",
        "java.nio.file.LinkPermission hard",
        "java.nio.file.LinkPermission symbolic",
        "java.security.SecurityPermission getDomainCombiner",
        "java.security.SecurityPermission getPolicy",
        "java.security.SecurityPermission setPolicy",
        "java.security.SecurityPermission getProperty.*",
        "java.security.SecurityPermission getProperty.foobar",
        "java.security.SecurityPermission setProperty.*",
        "java.security.SecurityPermission setProperty.foobar",
        "java.security.SecurityPermission removeProvider.*",
        "java.security.SecurityPermission removeProvider.foobar",
        "java.security.SecurityPermission clearProviderProperties.*",
        "java.security.SecurityPermission clearProviderProperties.foobar",
        "java.security.SecurityPermission removeProviderProperty.*",
        "java.security.SecurityPermission removeProviderProperty.foobar",
        "java.security.SecurityPermission insertProvider.*",
        "java.security.SecurityPermission insertProvider.foobar",
        "java.security.SecurityPermission setSystemScope",
        "java.security.SecurityPermission setIdentityPublicKey",
        "java.security.SecurityPermission setIdentityInfo",
        "java.security.SecurityPermission addIdentityCertificate",
        "java.security.SecurityPermission removeIdentityCertificate",
        "java.security.SecurityPermission printIdentity",
        "java.security.SecurityPermission getSignerPrivateKey",
        "java.security.SecurityPermission getSignerKeyPair",
        "java.sql.SQLPermission setLog",
        "java.sql.SQLPermission setSyncFactory",
        "java.sql.SQLPermission deregisterDriver",
        "java.util.logging.LoggingPermission control",
        "javax.management.MBeanPermission * getClassLoader",
        "javax.management.MBeanPermission * getClassLoaderFor",
        "javax.management.MBeanPermission * getClassLoaderRepository",
        "javax.management.MBeanTrustPermission *",
        "javax.management.remote.SubjectDelegationPermission *",
        "javax.net.ssl.SSLPermission setHostnameVerifier",
        "javax.net.ssl.SSLPermission getSSLSessionContext",
        "javax.net.ssl.SSLPermission setDefaultSSLContext",
        "javax.sound.sampled.AudioPermission play",
        "javax.sound.sampled.AudioPermission record",
        "javax.xml.bind.JAXBPermission setDatatypeConverter",
        "javax.xml.ws.WebServicePermission publishEndpoint"
    );

    public void testIllegalPermissions() throws Exception {
        assertIllegalPermissions(ILLEGAL_TEST_PERMISSIONS, PolicyUtil::getPluginPolicyInfo);
        assertIllegalPermissions(ILLEGAL_TEST_PERMISSIONS, PolicyUtil::getModulePolicyInfo);
    }

    public void testAllPermission() throws Exception {
        // AllPermission has no name element, so doesn't work with the format above
        Path tmpDir = createTempDir();
        assertIllegalPermission("java.security.AllPermission", null, null, tmpDir, PolicyUtil::getPluginPolicyInfo);
        assertIllegalPermission("java.security.AllPermission", null, null, tmpDir, PolicyUtil::getModulePolicyInfo);
    }
}
