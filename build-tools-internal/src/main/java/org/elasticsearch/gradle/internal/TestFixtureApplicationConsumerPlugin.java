/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Usage;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.testing.Test;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.elasticsearch.gradle.internal.TestFixtureApplicationPlugin.DISTRIBUTION_USAGE_ATTRIBUTE;

public class TestFixtureApplicationConsumerPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        FileCollection fixtureDistributions = project.getConfigurations().create("fixtureDistributions", configuration -> {
            configuration.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
            configuration.getAttributes()
                .attribute(Usage.USAGE_ATTRIBUTE, project.getObjects().named(Usage.class, DISTRIBUTION_USAGE_ATTRIBUTE));
        });

        project.getTasks().withType(Test.class).configureEach(test -> {
            TestFixtureApplicationConsumerTestExtension fixtureApps = test.getExtensions()
                .create("fixtureApps", TestFixtureApplicationConsumerTestExtension.class);
            // TODO we could filter this to only for tasks that have usages registered
            test.getInputs().files(fixtureDistributions);

            SystemPropertyCommandLineArgumentProvider nonInputSystemProperties = test.getExtensions()
                .findByType(SystemPropertyCommandLineArgumentProvider.class);

            BiConsumer<String, String> applySysProps = nonInputSystemProperties != null
                ? nonInputSystemProperties::systemProperty
                : test::systemProperty;

            test.doFirst(test1 -> {
                fixtureDistributions.getFiles().forEach(file -> {
                    String appKey = file.getName();
                    Collection<String> services = fixtureApps.getFixtureApplications().get(appKey);
                    if (services.isEmpty() == false) {
                        String appHomeKey = "fixture." + appKey + ".home";

                        System.out.println("appHomeKey = " + appHomeKey);
                        Path relativize = test.getWorkingDir().toPath().relativize(file.toPath());
                        System.out.println("relativize.toFile().getPath() = " + relativize.toFile().getPath());

                        applySysProps.accept(appHomeKey, relativize.toString());
                        AtomicInteger index = new AtomicInteger();
                        services.forEach(
                            (serviceName) -> applySysProps.accept("fixture." + appKey + ".service." + index.getAndIncrement(), serviceName)
                        );
                    }
                });
            });
        });
    }
}
