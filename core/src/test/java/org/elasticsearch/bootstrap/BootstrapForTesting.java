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

import org.apache.lucene.util.TestSecurityManager;
import org.elasticsearch.bootstrap.Bootstrap;
import org.elasticsearch.bootstrap.ESPolicy;
import org.elasticsearch.bootstrap.Security;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;

import java.io.FilePermission;
import java.net.URL;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Objects;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;

/** 
 * Initializes natives and installs test security manager
 * (init'd early by base classes to ensure it happens regardless of which
 * test case happens to be first, test ordering, etc). 
 * <p>
 * The idea is to mimic as much as possible what happens with ES in production
 * mode (e.g. assign permissions and install security manager the same way)
 */
public class BootstrapForTesting {
    
    // TODO: can we share more code with the non-test side here
    // without making things complex???

    static {
        // just like bootstrap, initialize natives, then SM
        Bootstrap.initializeNatives(true, true);

        // initialize probes
        Bootstrap.initializeProbes();
        
        // check for jar hell
        try {
            JarHell.checkJarHell();
        } catch (Exception e) {
            if (Boolean.parseBoolean(System.getProperty("tests.maven"))) {
                throw new RuntimeException("found jar hell in test classpath", e);
            } else {
                Loggers.getLogger(BootstrapForTesting.class)
                    .warn("Your ide or custom test runner has jar hell issues, " +
                          "you might want to look into that", e);
            }
        }

        // make sure java.io.tmpdir exists always (in case code uses it in a static initializer)
        Path javaTmpDir = PathUtils.get(Objects.requireNonNull(System.getProperty("java.io.tmpdir"),
                                                               "please set ${java.io.tmpdir} in pom.xml"));
        try {
            Security.ensureDirectoryExists(javaTmpDir);
        } catch (Exception e) {
            throw new RuntimeException("unable to create test temp directory", e);
        }

        // install security manager if requested
        if (systemPropertyAsBoolean("tests.security.manager", true)) {
            try {
                Security.setCodebaseProperties();
                // initialize paths the same exact way as bootstrap
                Permissions perms = new Permissions();
                // add permissions to everything in classpath
                for (URL url : JarHell.parseClassPath()) {
                    Path path = PathUtils.get(url.toURI());
                    // resource itself
                    perms.add(new FilePermission(path.toString(), "read,readlink"));
                    // classes underneath
                    perms.add(new FilePermission(path.toString() + path.getFileSystem().getSeparator() + "-", "read,readlink"));

                    // crazy jython...
                    String filename = path.getFileName().toString();
                    if (filename.contains("jython") && filename.endsWith(".jar")) {
                        // just enough so it won't fail when it does not exist
                        perms.add(new FilePermission(path.getParent().toString(), "read,readlink"));
                        perms.add(new FilePermission(path.getParent().resolve("Lib").toString(), "read,readlink"));
                    }
                }
                // java.io.tmpdir
                Security.addPath(perms, "java.io.tmpdir", javaTmpDir, "read,readlink,write,delete");
                // custom test config file
                if (Strings.hasLength(System.getProperty("tests.config"))) {
                    perms.add(new FilePermission(System.getProperty("tests.config"), "read,readlink"));
                }
                // jacoco coverage output file
                if (Boolean.getBoolean("tests.coverage")) {
                    Path coverageDir = PathUtils.get(System.getProperty("tests.coverage.dir"));
                    perms.add(new FilePermission(coverageDir.resolve("jacoco.exec").toString(), "read,write"));
                    // in case we get fancy and use the -integration goals later:
                    perms.add(new FilePermission(coverageDir.resolve("jacoco-it.exec").toString(), "read,write"));
                }

                // if its an insecure plugin, its not easy to simulate here, since we don't have a real plugin install.
                // we just do our best so unit testing can work. integration tests for such plugins are essential.
                String artifact = System.getProperty("tests.artifact");
                String insecurePluginProp = Security.INSECURE_PLUGINS.get(artifact);
                if (insecurePluginProp != null) {
                    setInsecurePluginPermissions(perms, insecurePluginProp);
                }
                Policy.setPolicy(new ESPolicy(perms));
                System.setSecurityManager(new TestSecurityManager());
                Security.selfTest();

                if (insecurePluginProp != null) {
                    // initialize the plugin class, in case it has one-time hacks (unit tests often won't do this)
                    String clazz = System.getProperty("tests.plugin.classname");
                    if (clazz == null) {
                        throw new IllegalStateException("plugin classname is needed for insecure plugin unit tests");
                    }
                    Class.forName(clazz);
                }
            } catch (Exception e) {
                throw new RuntimeException("unable to install test security manager", e);
            }
        }
    }

    /**
     * with a real plugin install, we just set a property to plugin/foo*, which matches
     * plugin code and all dependencies. when running unit tests, things are disorganized,
     * and might even be on different filesystem roots (windows), so we can't even make
     * a URL that will match everything. instead, add the extra permissions globally.
     */
    // TODO: maybe wrap with a policy so the extra permissions aren't applied to test classes/framework,
    // so that stacks are always polluted and tests fail for missing AccessController blocks...
    static void setInsecurePluginPermissions(Permissions permissions, String insecurePluginProp) throws Exception {
        // the hack begins!
        
        // parse whole policy file, with and without the substitution, compute the delta, then add globally.
        URL bogus = new URL("file:/bogus");
        ESPolicy policy = new ESPolicy(new Permissions());
        PermissionCollection small = policy.template.getPermissions(new CodeSource(bogus, (Certificate[])null));
        System.setProperty(insecurePluginProp, bogus.toString());
        policy = new ESPolicy(new Permissions());
        System.clearProperty(insecurePluginProp);
        PermissionCollection big = policy.template.getPermissions(new CodeSource(bogus, (Certificate[])null));
        
        PermissionCollection delta = delta(small, big);
        for (Permission p : Collections.list(delta.elements())) {
            permissions.add(p);
        }
    }
    
    // computes delta of small and big, the slow way
    static PermissionCollection delta(PermissionCollection small, PermissionCollection big) {
        Permissions extra = new Permissions();
        for (Permission p : Collections.list(big.elements())) {
            // check big too, to remove UnresolvedPermissions (acts like NaN)
            if (big.implies(p) && small.implies(p) == false) {
                extra.add(p);
            }
        }
        return extra;
    }

    // does nothing, just easy way to make sure the class is loaded.
    public static void ensureInitialized() {}
}
