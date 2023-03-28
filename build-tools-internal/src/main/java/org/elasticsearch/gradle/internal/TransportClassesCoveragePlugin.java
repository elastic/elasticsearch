/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import kotlinx.kover.KoverPlugin;
import kotlinx.kover.api.CounterType;
import kotlinx.kover.api.KoverProjectConfig;
import kotlinx.kover.api.VerificationTarget;
import kotlinx.kover.api.VerificationValueType;
import kotlinx.kover.tasks.KoverVerificationTask;

import org.elasticsearch.gradle.internal.precommit.transport.FindTransportClassesPlugin;
import org.elasticsearch.gradle.internal.precommit.transport.FindTransportClassesTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.TaskProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransportClassesCoveragePlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPluginManager().withPlugin("elasticsearch.build", plugin -> {
            project.getPluginManager().apply(KoverPlugin.class);
            project.getPluginManager().apply(FindTransportClassesPlugin.class);

            /*
            Kover applies itself to all tasks of type Test. see KoverProjectApplier.kt
                 tasks.withType<Test>().configureEach {
                 applyToTestTask(extension, engineProvider)
                }
                therefore the section below is trying to remove the expensive test tasks from the instrumentation
             */
            project.getExtensions().configure(KoverProjectConfig.class, kover -> {
                kover.instrumentation(instrumentation -> {
                    instrumentation.getExcludeTasks().add("internalClusterTest");
                    instrumentation.getExcludeTasks().add("yamlRestTest");
                    instrumentation.getExcludeTasks().add("yamlRestTestV7CompatTest");
                });
            });

            TaskProvider<KoverVerificationTask> koverVerify = project.getTasks().named("koverVerify", KoverVerificationTask.class);
            project.getTasks().named("check").configure(task -> task.dependsOn(koverVerify));

            koverVerify.configure(t -> {
                FindTransportClassesTask findTransportClassesTask = getFindTransportClassesTask(project);
                t.dependsOn(findTransportClassesTask);

                t.doFirst(t2 -> {
                    project.getExtensions().configure(KoverProjectConfig.class, kover -> {

                        kover.verify(verify -> {
                            verify.rule(rule -> {
                                rule.overrideClassFilter(koverClassFilter -> {
                                    Set<String> transportClasses = readAllLines(findTransportClassesTask.getTransportClasses());
                                    if (transportClasses.size() == 0) {
                                        koverClassFilter.getExcludes().add("*");
                                    } else {
                                        koverClassFilter.getIncludes().addAll(includes(transportClasses));
                                        koverClassFilter.getExcludes().addAll(excludes(transportClasses));
                                    }

                                });
                                rule.setTarget(VerificationTarget.CLASS);
                                rule.setEnabled(true);
                                rule.bound(bound -> {
                                    bound.setMinValue(100);
                                    bound.setCounter(CounterType.LINE);
                                    bound.setValueType(VerificationValueType.COVERED_PERCENTAGE);
                                });
                            });
                        });
                    });

                });
            });
            // adding a fake rule so that verification can run. the real rule is added with doFirst because of transportClass scanning being
            // done after tests
            project.getExtensions().configure(KoverProjectConfig.class, kover -> {
                // kover.getEngine().set(DefaultIntellijEngine.INSTANCE);
                kover.verify(verify -> {
                    verify.rule(rule -> {
                        rule.bound(bound -> {
                            bound.setMinValue(0);
                            bound.setCounter(CounterType.INSTRUCTION);
                            bound.setValueType(VerificationValueType.COVERED_PERCENTAGE);
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

    private Collection<String> includes(Set<String> transportClasses) {
        // when testing inner classes the enclosed class has to be included too
        return transportClasses.stream().flatMap(this::includeEnclosingClassForInner)
            .collect(Collectors.toSet());
    }

    private Stream<String> includeEnclosingClassForInner(String name) {
        if (name.contains("$")) {
            return Stream.of(name, name.substring(0, name.indexOf('$')));
        }
        return Stream.of(name);
    }

    private Collection<String> excludes(Set<String> transportClasses) {
        // when inner class was a transport class its enclosing class had to be included (see #includes)
        // but if an enclosing class is not a transport class it should be excluded
        return transportClasses.stream()
            .filter(name -> name.contains("$"))
            .map(name -> name.substring(0, name.indexOf('$'))) // get enclosing name
            .filter(name -> transportClasses.contains(name) == false)
            .collect(Collectors.toSet());
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
