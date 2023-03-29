/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.precommit.transport.FindTransportClassesPlugin;
import org.elasticsearch.gradle.internal.precommit.transport.FindTransportClassesTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.testing.jacoco.plugins.JacocoPlugin;
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension;
import org.gradle.testing.jacoco.tasks.JacocoCoverageVerification;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportClassesCoveragePlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(TransportClassesCoveragePlugin.class);

    @Override
    public void apply(Project project) {
        project.getPluginManager().withPlugin("elasticsearch.build", plugin -> {
            project.getRepositories().maven(maven -> { maven.setUrl("https://oss.sonatype.org/content/repositories/snapshots/"); });

            project.getPluginManager().apply(JacocoPlugin.class);
            project.getPluginManager().apply(FindTransportClassesPlugin.class);

            // support for java 20
            project.getExtensions().getByType(JacocoPluginExtension.class).setToolVersion("0.8.9-20230327.073737-36");

            // this is suggested by gradle jacoco doc https://docs.gradle.org/current/userguide/jacoco_plugin.html
            project.getTasks().named("test").configure(task -> { task.finalizedBy(project.getTasks().named("jacocoTestReport")); });
            TaskProvider<Task> jacocoTestReport = project.getTasks().named("jacocoTestReport");
            jacocoTestReport.configure(task -> { task.dependsOn(project.getTasks().named("test")); });

            TaskProvider<JacocoCoverageVerification> verify = project.getTasks()
                .named("jacocoTestCoverageVerification", JacocoCoverageVerification.class);
            project.getTasks().named("check").configure(task -> task.dependsOn(verify));

            verify.configure(t -> {
                FindTransportClassesTask findTransportClassesTask = getFindTransportClassesTask(project);
                t.dependsOn(jacocoTestReport);
                t.dependsOn(findTransportClassesTask);

                t.doFirst(aTask -> {
                    var task = (JacocoCoverageVerification) aTask;
                    task.getViolationRules().rule(jacocoViolationRule -> {

                        Set<String> transportClasses = readAllLines(findTransportClassesTask.getTransportClasses());
                        jacocoViolationRule.setElement("CLASS");
                        jacocoViolationRule.limit(l -> {
                            l.setCounter("LINE");
                            l.setValue("COVEREDRATIO");
                            l.setMinimum(BigDecimal.valueOf(0.1));
                        });
                        List<String> includes = includes(transportClasses);
                        jacocoViolationRule.setIncludes(includes);
                        LOGGER.info(String.format(Locale.ROOT, "classes included in coverage rule %s", includes));
                    });
                    // adding a fake rule so that verification can run.
                    // the real rule is added with doFirst because of transportClass scanning being done after tests
                    task.getViolationRules().rule(jacocoViolationRule -> {
                        jacocoViolationRule.limit(l -> {
                            l.setCounter("LINE");
                            l.setValue("COVEREDRATIO");
                            l.setMinimum(BigDecimal.valueOf(0.0));
                        });
                    });

                });
            });

        });

    }

    private static FindTransportClassesTask getFindTransportClassesTask(Project project) {
        FindTransportClassesTask findTransportClassesTask = project.getTasks()
            .named("findTransportClassesTask", FindTransportClassesTask.class)
            .get();
        return findTransportClassesTask;
    }

    private List<String> includes(Set<String> transportClasses) {
        return transportClasses.stream().map(this::includeEnclosingClassForInner).collect(Collectors.toList());
    }

    private String includeEnclosingClassForInner(String name) {
        if (name.contains("$")) {
            return escapeDollar(name);
        }
        return name;
    }

    private String escapeDollar(String name) {
        return name.replace("$", ".");
    }

    private static Set<String> readAllLines(RegularFileProperty file) {
        try {
            Path path = file.get().getAsFile().toPath();
            return Files.readAllLines(path).stream().collect(Collectors.toSet());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptySet();
    }
}
