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

package org.elasticsearch.gradle

import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

import static org.junit.Assert.assertTrue
import static org.junit.Assert.assertEquals

class SimplePluginIT {

    @Rule
    public TemporaryFolder testProjectDir = new TemporaryFolder()

    File buildFile
    List<File> pluginClasspath

    @Before
    public void setup() throws IOException {
        buildFile = testProjectDir.newFile("build.gradle")
        URL pluginClasspathResource = getClass().classLoader.findResource("plugin-classpath.txt")
        if (pluginClasspathResource == null) {
            throw new IllegalStateException("Plugin classpath resource not found, run `createPluginClasspathManifest` task in buildSrc.")
        }
        pluginClasspath = pluginClasspathResource.readLines("UTF-8").collect { new File(it) }
    }

    @Test
    public void testSimplePlugin() throws IOException {
        buildFile.setText("""

            plugins {
                id 'elasticsearch.esplugin'
            }

            esplugin {
                description 'Dummy description.'
                classname 'org.elasticsearch.plugin.dummy.DummyPlugin'
            }

            """, 'UTF-8');

        BuildResult result = GradleRunner.create()
            .withPluginClasspath(pluginClasspath)
            .withProjectDir(testProjectDir.getRoot())
            .withArguments("assemble")
            .build()

        assertTrue(result.getOutput().contains("Elasticsearch Build Hamster says Hello!"))
        assertEquals(result.task(":assemble").getOutcome(), TaskOutcome.SUCCESS)
    }

}
