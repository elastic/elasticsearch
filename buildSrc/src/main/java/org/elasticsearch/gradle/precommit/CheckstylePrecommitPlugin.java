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

package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.util.Util;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.quality.Checkstyle;
import org.gradle.api.plugins.quality.CheckstyleExtension;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class CheckstylePrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        // Always copy the checkstyle configuration files to 'buildDir/checkstyle' since the resources could be located in a jar
        // file. If the resources are located in a jar, Gradle will fail when it tries to turn the URL into a file
        URL checkstyleConfUrl = CheckstylePrecommitPlugin.class.getResource("/checkstyle.xml");
        URL checkstyleSuppressionsUrl = CheckstylePrecommitPlugin.class.getResource("/checkstyle_suppressions.xml");
        File checkstyleDir = new File(project.getBuildDir(), "checkstyle");
        File checkstyleSuppressions = new File(checkstyleDir, "checkstyle_suppressions.xml");
        File checkstyleConf = new File(checkstyleDir, "checkstyle.xml");
        TaskProvider<Task> copyCheckstyleConf = project.getTasks().register("copyCheckstyleConf");

        // configure inputs and outputs so up to date works properly
        copyCheckstyleConf.configure(t -> t.getOutputs().files(checkstyleSuppressions, checkstyleConf));
        if ("jar".equals(checkstyleConfUrl.getProtocol())) {
            try {
                JarURLConnection jarURLConnection = (JarURLConnection) checkstyleConfUrl.openConnection();
                copyCheckstyleConf.configure(t -> t.getInputs().file(jarURLConnection.getJarFileURL()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else if ("file".equals(checkstyleConfUrl.getProtocol())) {
            copyCheckstyleConf.configure(t -> t.getInputs().files(checkstyleConfUrl.getFile(), checkstyleSuppressionsUrl.getFile()));
        }

        copyCheckstyleConf.configure(t -> t.doLast(task -> {
            checkstyleDir.mkdirs();
            try (InputStream stream = checkstyleConfUrl.openStream()) {
                Files.copy(stream, checkstyleConf.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            try (InputStream stream = checkstyleSuppressionsUrl.openStream()) {
                Files.copy(stream, checkstyleSuppressions.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));

        TaskProvider<Task> checkstyleTask = project.getTasks().register("checkstyle");
        checkstyleTask.configure(t -> t.dependsOn(project.getTasks().withType(Checkstyle.class)));

        // Apply the checkstyle plugin to create `checkstyleMain` and `checkstyleTest`. It only
        // creates them if there is main or test code to check and it makes `check` depend
        // on them. We also want `precommit` to depend on `checkstyle`.
        project.getPluginManager().apply("checkstyle");
        CheckstyleExtension checkstyle = project.getExtensions().getByType(CheckstyleExtension.class);
        checkstyle.getConfigDirectory().set(checkstyleDir);

        DependencyHandler dependencies = project.getDependencies();
        String checkstyleVersion = VersionProperties.getVersions().get("checkstyle");
        dependencies.add("checkstyle", "com.puppycrawl.tools:checkstyle:" + checkstyleVersion);
        dependencies.add("checkstyle", project.files(Util.getBuildSrcCodeSource()));

        project.getTasks().withType(Checkstyle.class).configureEach(t -> {
            t.dependsOn(copyCheckstyleConf);
            t.reports(r -> r.getHtml().setEnabled(false));
        });

        return checkstyleTask;
    }
}
