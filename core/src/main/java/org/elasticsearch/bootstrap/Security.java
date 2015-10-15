/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginInfo;

import java.io.*;
import java.net.URL;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.URIParameter;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** 
 * Initializes SecurityManager with necessary permissions.
 * <p>
 * <h1>Initialization</h1>
 * The JVM is not initially started with security manager enabled,
 * instead we turn it on early in the startup process. This is a tradeoff
 * between security and ease of use:
 * <ul>
 *   <li>Assigns file permissions to user-configurable paths that can
 *       be specified from the command-line or {@code elasticsearch.yml}.</li>
 *   <li>Allows for some contained usage of native code that would not
 *       otherwise be permitted.</li>
 * </ul>
 * <p>
 * <h1>Permissions</h1>
 * Permissions use a policy file packaged as a resource, this file is
 * also used in tests. File permissions are generated dynamically and
 * combined with this policy file.
 * <p>
 * For each configured path, we ensure it exists and is accessible before
 * granting permissions, otherwise directory creation would require
 * permissions to parent directories.
 * <p>
 * In some exceptional cases, permissions are assigned to specific jars only,
 * when they are so dangerous that general code should not be granted the
 * permission, but there are extenuating circumstances.
 * <p>
 * Scripts (groovy, javascript, python) are assigned minimal permissions. This does not provide adequate
 * sandboxing, as these scripts still have access to ES classes, and could
 * modify members, etc that would cause bad things to happen later on their
 * behalf (no package protections are yet in place, this would need some
 * cleanups to the scripting apis). But still it can provide some defense for users
 * that enable dynamic scripting without being fully aware of the consequences.
 * <p>
 * <h1>Disabling Security</h1>
 * SecurityManager can be disabled completely with this setting:
 * <pre>
 * es.security.manager.enabled = false
 * </pre>
 * <p>
 * <h1>Debugging Security</h1>
 * A good place to start when there is a problem is to turn on security debugging:
 * <pre>
 * JAVA_OPTS="-Djava.security.debug=access,failure" bin/elasticsearch
 * </pre>
 * See <a href="https://docs.oracle.com/javase/7/docs/technotes/guides/security/troubleshooting-security.html">
 * Troubleshooting Security</a> for information.
 */
final class Security {
    /** no instantiation */
    private Security() {}
       
    /** 
     * Initializes SecurityManager for the environment
     * Can only happen once!
     */
    static void configure(Environment environment) throws Exception {
        // set properties for jar locations
        setCodebaseProperties();

        // enable security policy: union of template and environment-based paths, and possibly plugin permissions
        Policy.setPolicy(new ESPolicy(createPermissions(environment), getPluginPermissions(environment)));

        // enable security manager
        System.setSecurityManager(new SecurityManager() {
            // we disable this completely, because its granted otherwise:
            // 'Note: The "exitVM.*" permission is automatically granted to
            // all code loaded from the application class path, thus enabling
            // applications to terminate themselves.'
            @Override
            public void checkExit(int status) {
                throw new SecurityException("exit(" + status + ") not allowed by system policy");
            }
        });

        // do some basic tests
        selfTest();
    }

    // mapping of jars to codebase properties
    // note that this is only read once, when policy is parsed.
    private static final Map<Pattern,String> SPECIAL_JARS;
    static {
        Map<Pattern,String> m = new IdentityHashMap<>();
        m.put(Pattern.compile(".*lucene-core-.*\\.jar$"),              "es.security.jar.lucene.core");
        m.put(Pattern.compile(".*jsr166e-.*\\.jar$"),                  "es.security.jar.twitter.jsr166e");
        m.put(Pattern.compile(".*lucene-test-framework-.*\\.jar$"),    "es.security.jar.lucene.testframework");
        m.put(Pattern.compile(".*randomizedtesting-runner-.*\\.jar$"), "es.security.jar.randomizedtesting.runner");
        m.put(Pattern.compile(".*junit4-ant-.*\\.jar$"),               "es.security.jar.randomizedtesting.junit4");
        m.put(Pattern.compile(".*securemock-.*\\.jar$"),               "es.security.jar.elasticsearch.securemock");
        SPECIAL_JARS = Collections.unmodifiableMap(m);
    }

    /**
     * Sets properties (codebase URLs) for policy files.
     * JAR locations are not fixed so we have to find the locations of
     * the ones we want.
     */
    @SuppressForbidden(reason = "proper use of URL")
    static void setCodebaseProperties() {
        for (URL url : JarHell.parseClassPath()) {
            for (Map.Entry<Pattern,String> e : SPECIAL_JARS.entrySet()) {
                if (e.getKey().matcher(url.getPath()).matches()) {
                    String prop = e.getValue();
                    if (System.getProperty(prop) != null) {
                        throw new IllegalStateException("property: " + prop + " is unexpectedly set: " + System.getProperty(prop));
                    }
                    System.setProperty(prop, url.toString());
                }
            }
        }
        for (String prop : SPECIAL_JARS.values()) {
            if (System.getProperty(prop) == null) {
                System.setProperty(prop, "file:/dev/null"); // no chance to be interpreted as "all"
            }
        }
    }

    /**
     * Sets properties (codebase URLs) for policy files.
     * we look for matching plugins and set URLs to fit
     */
    @SuppressForbidden(reason = "proper use of URL")
    static Map<String,PermissionCollection> getPluginPermissions(Environment environment) throws IOException, NoSuchAlgorithmException {
        Map<String,PermissionCollection> map = new HashMap<>();
        if (Files.exists(environment.pluginsFile())) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(environment.pluginsFile())) {
                for (Path plugin : stream) {
                    Path policyFile = plugin.resolve(PluginInfo.ES_PLUGIN_POLICY);
                    if (Files.exists(policyFile)) {
                        // parse the plugin's policy file into a set of permissions
                        Policy policy = Policy.getInstance("JavaPolicy", new URIParameter(policyFile.toUri()));
                        PermissionCollection permissions = policy.getPermissions(Security.class.getProtectionDomain());
                        // this method is supported with the specific implementation we use, but just check for safety.
                        if (permissions == Policy.UNSUPPORTED_EMPTY_COLLECTION) {
                            throw new UnsupportedOperationException("JavaPolicy implementation does not support retrieving permissions");
                        }
                        // grant the permissions to each jar in the plugin
                        try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(plugin, "*.jar")) {
                            for (Path jar : jarStream) {
                                if (map.put(jar.toUri().toURL().getFile(), permissions) != null) {
                                    // just be paranoid ok?
                                    throw new IllegalStateException("per-plugin permissions already granted for jar file: " + jar);
                                }
                            }
                        }
                    }
                }
            }
        }
        return Collections.unmodifiableMap(map);
    }

    /** returns dynamic Permissions to configured paths */
    static Permissions createPermissions(Environment environment) throws IOException {
        Permissions policy = new Permissions();
        // read-only dirs
        addPath(policy, "path.home", environment.binFile(), "read,readlink");
        addPath(policy, "path.home", environment.libFile(), "read,readlink");
        addPath(policy, "path.plugins", environment.pluginsFile(), "read,readlink");
        addPath(policy, "path.conf", environment.configFile(), "read,readlink");
        addPath(policy, "path.scripts", environment.scriptsFile(), "read,readlink");
        // read-write dirs
        addPath(policy, "java.io.tmpdir", environment.tmpFile(), "read,readlink,write,delete");
        addPath(policy, "path.logs", environment.logsFile(), "read,readlink,write,delete");
        if (environment.sharedDataFile() != null) {
            addPath(policy, "path.shared_data", environment.sharedDataFile(), "read,readlink,write,delete");
        }
        for (Path path : environment.dataFiles()) {
            addPath(policy, "path.data", path, "read,readlink,write,delete");
        }
        for (Path path : environment.dataWithClusterFiles()) {
            addPath(policy, "path.data", path, "read,readlink,write,delete");
        }
        for (Path path : environment.repoFiles()) {
            addPath(policy, "path.repo", path, "read,readlink,write,delete");
        }
        if (environment.pidFile() != null) {
            // we just need permission to remove the file if its elsewhere.
            policy.add(new FilePermission(environment.pidFile().toString(), "delete"));
        }
        return policy;
    }
    
    /**
     * Add access to path (and all files underneath it)
     * @param policy current policy to add permissions to
     * @param configurationName the configuration name associated with the path (for error messages only)
     * @param path the path itself
     * @param permissions set of filepermissions to grant to the path
     */
    static void addPath(Permissions policy, String configurationName, Path path, String permissions) {
        // paths may not exist yet, this also checks accessibility
        try {
            ensureDirectoryExists(path);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to access '" + configurationName + "' (" + path + ")", e);
        }

        // add each path twice: once for itself, again for files underneath it
        policy.add(new FilePermission(path.toString(), permissions));
        policy.add(new FilePermission(path.toString() + path.getFileSystem().getSeparator() + "-", permissions));
    }
    
    /**
     * Ensures configured directory {@code path} exists.
     * @throws IOException if {@code path} exists, but is not a directory, not accessible, or broken symbolic link.
     */
    static void ensureDirectoryExists(Path path) throws IOException {
        // this isn't atomic, but neither is createDirectories.
        if (Files.isDirectory(path)) {
            // verify access, following links (throws exception if something is wrong)
            // we only check READ as a sanity test
            path.getFileSystem().provider().checkAccess(path.toRealPath(), AccessMode.READ);
        } else {
            // doesn't exist, or not a directory
            try {
                Files.createDirectories(path);
            } catch (FileAlreadyExistsException e) {
                // convert optional specific exception so the context is clear
                IOException e2 = new NotDirectoryException(path.toString());
                e2.addSuppressed(e);
                throw e2;
            }
        }
    }

    /** Simple checks that everything is ok */
    @SuppressForbidden(reason = "accesses jvm default tempdir as a self-test")
    static void selfTest() throws IOException {
        // check we can manipulate temporary files
        try {
            Path p = Files.createTempFile(null, null);
            try {
                Files.delete(p);
            } catch (IOException ignored) {
                // potentially virus scanner
            }
        } catch (SecurityException problem) {
            throw new SecurityException("Security misconfiguration: cannot access java.io.tmpdir", problem);
        }
    }
}
