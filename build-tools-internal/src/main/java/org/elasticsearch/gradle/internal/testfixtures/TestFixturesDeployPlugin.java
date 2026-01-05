/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.testfixtures;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.internal.docker.DockerBuildTask;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

public class TestFixturesDeployPlugin implements Plugin<Project> {

    public static final String DEPLOY_FIXTURE_TASK_NAME = "deployFixtureDockerImages";
    private static String DEFAULT_DOCKER_REGISTRY = "docker.elastic.co/elasticsearch-dev";

    @Override
    public void apply(Project project) {
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        var buildParams = loadBuildParams(project).get();
        NamedDomainObjectContainer<TestFixtureDeployment> fixtures = project.container(TestFixtureDeployment.class);
        project.getExtensions().add("dockerFixtures", fixtures);
        registerDeployTaskPerFixture(project, fixtures, buildParams.getCi());
        project.getTasks().register(DEPLOY_FIXTURE_TASK_NAME, task -> task.dependsOn(project.getTasks().withType(DockerBuildTask.class)));
    }

    private static void registerDeployTaskPerFixture(
        Project project,
        NamedDomainObjectContainer<TestFixtureDeployment> fixtures,
        boolean isCi
    ) {
        fixtures.all(
            fixture -> project.getTasks()
                .register("deploy" + StringUtils.capitalize(fixture.getName()) + "DockerImage", DockerBuildTask.class, task -> {
                    task.getDockerContext().fileValue(fixture.getDockerContext().get());
                    List<String> baseImages = fixture.getBaseImages().get();
                    if (baseImages.isEmpty() == false) {
                        task.setBaseImages(baseImages.toArray(new String[baseImages.size()]));
                    }
                    task.setNoCache(isCi);
                    task.setTags(
                        new String[] {
                            resolveTargetDockerRegistry(fixture) + "/" + fixture.getName() + "-fixture:" + fixture.getVersion().get() }
                    );
                    task.getPush().set(isCi);
                    task.getPlatforms().addAll(Arrays.stream(Architecture.values()).map(a -> a.dockerPlatform).toList());
                    task.setGroup("Deploy TestFixtures");
                    task.setDescription("Deploys the " + fixture.getName() + " test fixture");
                })
        );
    }

    private static String resolveTargetDockerRegistry(TestFixtureDeployment fixture) {
        return fixture.getDockerRegistry().getOrElse(DEFAULT_DOCKER_REGISTRY);
    }
}
