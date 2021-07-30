/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.InternalPlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.quality.Checkstyle;
import org.gradle.api.plugins.quality.CheckstyleExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class CheckstylePrecommitPlugin extends PrecommitPlugin implements InternalPlugin {
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

        // Explicitly using an Action interface as java lambdas
        // are not supported by Gradle up-to-date checks
        copyCheckstyleConf.configure(t -> t.doLast(new Action<Task>() {
            @Override
            public void execute(Task task) {
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
        Provider<String> dependencyProvider = project.provider(() -> "org.elasticsearch:build-conventions:" + project.getVersion());
        dependencies.add("checkstyle", "com.puppycrawl.tools:checkstyle:" + checkstyleVersion);
        dependencies.addProvider("checkstyle", dependencyProvider, dep -> dep.setTransitive(false));

        project.getTasks().withType(Checkstyle.class).configureEach(t -> {
            t.dependsOn(copyCheckstyleConf);
            t.reports(r -> r.getHtml().getRequired().set(false));
        });

        return checkstyleTask;
    }

    private static URI getBuildSrcCodeSource() {
        try {
            return CheckstylePrecommitPlugin.class.getProtectionDomain().getCodeSource().getLocation().toURI();
        } catch (URISyntaxException e) {
            throw new GradleException("Error determining build tools JAR location", e);
        }
    }
}
