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
        if (systemPropertyAsBoolean("tests.security.manager", false)) {
            try {
                Security.setCodebaseProperties();
                // initialize paths the same exact way as bootstrap.
                Permissions perms = new Permissions();
                Path basedir = PathUtils.get(Objects.requireNonNull(System.getProperty("project.basedir"), 
                                                                    "please set ${project.basedir} in pom.xml"));
                // target/classes, target/test-classes
                Security.addPath(perms, basedir.resolve("target").resolve("classes"), "read,readlink");
                Security.addPath(perms, basedir.resolve("target").resolve("test-classes"), "read,readlink");
                // .m2/repository
                Path m2repoDir = PathUtils.get(Objects.requireNonNull(System.getProperty("m2.repository"), 
                                                                     "please set ${m2.repository} in pom.xml"));
                Security.addPath(perms, m2repoDir, "read,readlink");
                // java.io.tmpdir
                Security.addPath(perms, javaTmpDir, "read,readlink,write,delete");
                // custom test config file
                if (Strings.hasLength(System.getProperty("tests.config"))) {
                    perms.add(new FilePermission(System.getProperty("tests.config"), "read,readlink"));
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
