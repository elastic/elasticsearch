package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;

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
public class JarHellTaskIT extends GradleIntegrationTestCase {

    public void testJarHellDetected() {
        BuildResult result = getGradleRunner("jarHell")
            .withArguments("clean", "precommit", "-s", "-Dlocal.repo.path=" + getLocalTestRepoPath())
            .buildAndFail();

        assertTaskFailed(result, ":jarHell");
        assertOutputContains(
            result.getOutput(),
            "java.lang.IllegalStateException: jar hell!",
            "class: org.apache.logging.log4j.Logger"
        );
    }

}
