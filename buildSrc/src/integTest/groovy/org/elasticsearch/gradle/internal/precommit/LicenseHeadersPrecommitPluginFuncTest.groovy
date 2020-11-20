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

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class LicenseHeadersPrecommitPluginFuncTest extends AbstractGradleFuncTest {

    def "detects invalid files with invalid license header"() {
        given:
        buildFile << """
        plugins {
            id 'java'
            id 'elasticsearch.internal-licenseheaders'
        }
        """
        apacheSourceFile()
        invalidSourceFile()

        when:
        def result = gradleRunner("licenseHeaders").buildAndFail()

        then:
        result.task(":licenseHeaders").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, "> License header problems were found! Full details: ./build/reports/licenseHeaders/rat.xml")
        assertOutputContains(result.output, "./src/main/java/org/acme/InvalidLicensed.java")
        normalizedOutput(result.output).contains("./src/main/java/org/acme/ApacheLicensed.java") == false
    }

    def "can filter source files"() {
        given:
        buildFile << """
        plugins {
            id 'java'
            id 'elasticsearch.internal-licenseheaders'
        }
        
        tasks.named("licenseHeaders").configure {
            excludes << 'org/acme/filtered/**/*'
        }
        """
        apacheSourceFile()
        invalidSourceFile("src/main/java/org/acme/filtered/FilteredInvalidLicensed.java")

        when:
        def result = gradleRunner("licenseHeaders").build()

        then:
        result.task(":licenseHeaders").outcome == TaskOutcome.SUCCESS
//        assertOutputContains(result.output, "> License header problems were found! Full details: ./build/reports/licenseHeaders/rat.xml")
//        assertOutputContains(result.output, "./src/main/java/org/acme/InvalidLicensed.java")
//        normalizedOutput(result.output).contains("./src/main/java/org/acme/ApacheLicensed.java") == false
    }

    private File invalidSourceFile(String filePath = "src/main/java/org/acme/InvalidLicensed.java") {
        File sourceFile = file(filePath);
        sourceFile << """
/*
 * Blubb my custom license shrug!
 */
 
 package org.acme;
 
 public class ${sourceFile.getName() - ".java"} {
 }
 """
    }

    private File apacheSourceFile() {
        file("src/main/java/org/acme/ApacheLicensed.java") << """
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
 
 package org.acme;
 
 public class ApacheLicensed {
 }
 """
    }
}
