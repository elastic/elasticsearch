/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.reaper

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class ReaperPluginFuncTest extends AbstractGradleFuncTest {

    def "can launch reaper"() {
        given:
        buildFile << """
            plugins {
              id 'elasticsearch.reaper'
            }

            import org.elasticsearch.gradle.ReaperPlugin;
            import org.elasticsearch.gradle.util.GradleUtils;

            def serviceProvider = GradleUtils.getBuildService(project.getGradle().getSharedServices(), ReaperPlugin.REAPER_SERVICE_NAME);

            tasks.register("launchReaper") {
              doLast {
                def reaper = serviceProvider.get()
                reaper.registerCommand('test', 'true')
                reaper.unregister('test')
              }
            }
        """
        when:
        def result = gradleRunner(":launchReaper", "-S", "--info").build()
        then:
        result.task(":launchReaper").outcome == TaskOutcome.SUCCESS
        result.output.contains("Copying reaper.jar...")
        result.output.contains("Launching reaper:")
        result.output.contains("Waiting for reaper to exit normally")
    }
}
