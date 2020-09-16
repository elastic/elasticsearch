/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class InternalDistributionBwcSetupPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        settingsFile << """
            // project paths referenced absolute from BwcVersions.UnreleasedVersionInfo%gradleProjectPath
            include ":distribution:bwc:bugfix"
            include ":distribution:bwc:minor"
        """
    }

    def "applies common configuration to bwc subprojects"() {
        given:
        internalBuild();
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'  
        """
        when:
        def result = gradleRunner("assemble").build()
        then:
        result.task(":distribution:bwc:bugfix:assemble").outcome == TaskOutcome.SKIPPED
        result.task(":distribution:bwc:minor:assemble").outcome == TaskOutcome.SKIPPED
    }
}
