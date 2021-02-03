/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.test

import groovy.transform.CompileStatic
import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.testclusters.TestClustersPlugin
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin

/**
 * Adds support for starting an Elasticsearch cluster before running integration
 * tests. Used in conjunction with {@link StandaloneRestTestPlugin} for qa
 * projects and in conjunction with {@link BuildPlugin} for testing the rest
 * client.
 */
@CompileStatic
class RestTestPlugin implements Plugin<Project> {
    List<String> REQUIRED_PLUGINS = [
        'elasticsearch.build',
        'elasticsearch.standalone-rest-test']

    @Override
    void apply(Project project) {
        if (false == REQUIRED_PLUGINS.any { project.pluginManager.hasPlugin(it) }) {
            throw new InvalidUserDataException('elasticsearch.rest-test '
                + 'requires either elasticsearch.build or '
                + 'elasticsearch.standalone-rest-test')
        }
        project.getPlugins().apply(RestTestBasePlugin.class);
        project.pluginManager.apply(TestClustersPlugin)
        RestIntegTestTask integTest = project.tasks.create('integTest', RestIntegTestTask.class)
        integTest.description = 'Runs rest tests against an elasticsearch cluster.'
        integTest.group = JavaBasePlugin.VERIFICATION_GROUP
        integTest.mustRunAfter(project.tasks.named('precommit'))
        project.tasks.named('check').configure { it.dependsOn(integTest) }
    }
}
