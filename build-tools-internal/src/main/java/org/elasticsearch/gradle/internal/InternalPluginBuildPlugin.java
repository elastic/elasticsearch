/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.precommit.TestingConventionsPrecommitPlugin;
import org.elasticsearch.gradle.internal.precommit.TestingConventionsTasks;
import org.gradle.api.Project;

public class InternalPluginBuildPlugin implements InternalPlugin {
    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(BuildPlugin.class);
        project.getPluginManager().apply(BaseInternalPluginBuildPlugin.class);

        project.getPlugins()
            .withType(
                TestingConventionsPrecommitPlugin.class,
                plugin -> project.getTasks().withType(TestingConventionsTasks.class).named("testingConventions").configure(t -> {
                    t.getNaming().clear();
                    t.getNaming()
                        .create(
                            "Tests",
                            testingConventionRule -> testingConventionRule.baseClass("org.apache.lucene.tests.util.LuceneTestCase")
                        );
                    t.getNaming().create("IT", testingConventionRule -> {
                        testingConventionRule.baseClass("org.elasticsearch.test.ESIntegTestCase");
                        testingConventionRule.baseClass("org.elasticsearch.test.rest.ESRestTestCase");
                        testingConventionRule.baseClass("org.elasticsearch.test.ESSingleNodeTestCase");
                    });
                })
            );
    }
}
