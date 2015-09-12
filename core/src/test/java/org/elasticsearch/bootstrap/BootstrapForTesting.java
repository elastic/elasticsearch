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
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.Permissions;
import java.security.Policy;
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
                // initialize paths the same exact way as bootstrap.
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
                Policy.setPolicy(new ESPolicy(perms));
                System.setSecurityManager(new TestSecurityManager());
                Security.selfTest();
            } catch (Exception e) {
                throw new RuntimeException("unable to install test security manager", e);
            }
        }
    }

    // does nothing, just easy way to make sure the class is loaded.
    public static void ensureInitialized() {}
}
