/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;

/**
 * This plugin registers a {@link ProjectSubscribeBuildService} to allow projects to
 * communicate with each other during the configuration phase.
 *
 * For example, a project can register itself as a publisher of a topic, and other
 * projects can resolve projects that have registered as publishers of that topic.
 *
 * The actual resolution of whatever data is usually done using dependency declarations.
 * Be aware the state of the list depends on the order of project configuration and
 * consuming on configuration phase before task graph calculation phase should be avoided.
 *
 * We want to avoid discouraged plugin api usage like project.allprojects or project.subprojects
 * in plugins to avoid unnecessary configuration of projects and not break project isolation and break
 * See https://docs.gradle.org/current/userguide/isolated_projects.html
 * */
public class ProjectSubscribeServicePlugin implements Plugin<Project> {

    private Provider<ProjectSubscribeBuildService> publishSubscribe;

    @Override
    public void apply(Project project) {
        publishSubscribe = project.getGradle().getSharedServices().registerIfAbsent("publishSubscribe", ProjectSubscribeBuildService.class);
    }

    public Provider<ProjectSubscribeBuildService> getService() {
        return publishSubscribe;
    }

}
