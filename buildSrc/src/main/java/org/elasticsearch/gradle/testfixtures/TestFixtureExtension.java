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
package org.elasticsearch.gradle.testfixtures;

import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;

public class TestFixtureExtension {

    private final Project project;
    final NamedDomainObjectContainer<Project> fixtures;

    public TestFixtureExtension(Project project) {
        this.project = project;
        this.fixtures = project.container(Project.class);
    }

    public void useFixture(String path) {
        Project fixtureProject = this.project.findProject(path);
        if (fixtureProject == null) {
            throw new IllegalArgumentException("Could not find test fixture " + fixtureProject);
        }
        if (fixtureProject.file(TestFixturesPlugin.DOCKER_COMPOSE_YML).exists() == false) {
            throw new IllegalArgumentException(
                "Project " + path + " is not a valid test fixture: missing " + TestFixturesPlugin.DOCKER_COMPOSE_YML
            );
        }
        fixtures.add(fixtureProject);
    }

}
