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

package org.elasticsearch.gradle.test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.junit.Assert.fail;

public class TestClasspathUtils {

    public static void setupJarJdkClasspath(File projectRoot) {
        try {
            URL originLocation = TestClasspathUtils.class.getClassLoader()
                .loadClass("org.elasticsearch.bootstrap.JdkJarHellCheck")
                .getProtectionDomain()
                .getCodeSource()
                .getLocation();
            File targetFile = new File(
                projectRoot,
                "sample_jars/build/testrepo/org/elasticsearch/elasticsearch-core/current/elasticsearch-core-current.jar"
            );
            targetFile.getParentFile().mkdirs();
            Path originalPath = Paths.get(originLocation.toURI());
            Files.copy(originalPath, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (ClassNotFoundException | URISyntaxException | IOException e) {
            e.printStackTrace();
            fail("Cannot setup jdk jar hell classpath");
        }
    }

}
