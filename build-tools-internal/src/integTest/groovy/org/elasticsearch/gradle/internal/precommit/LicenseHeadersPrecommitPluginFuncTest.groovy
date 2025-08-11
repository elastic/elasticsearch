/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradlePrecommitPluginFuncTest
import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersPrecommitPlugin
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome

class LicenseHeadersPrecommitPluginFuncTest extends AbstractGradlePrecommitPluginFuncTest {

    Class<? extends PrecommitPlugin> pluginClassUnderTest = LicenseHeadersPrecommitPlugin.class

    def setup() {
        buildFile << """
        apply plugin:'java'
        """
    }
    def "detects invalid files with invalid license header"() {
        given:
        dualLicensedFile()
        unknownSourceFile()
        unapprovedSourceFile()

        when:
        def result = gradleRunner("licenseHeaders").buildAndFail()

        then:
        result.task(":licenseHeaders").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, "> Check failed. License header problems were found. Full details: ./build/reports/licenseHeaders/rat.xml")
        assertOutputContains(result.output, "./src/main/java/org/acme/UnknownLicensed.java")
        assertOutputContains(result.output, "./src/main/java/org/acme/UnapprovedLicensed.java")
        result.output.contains("./src/main/java/org/acme/DualLicensed.java") == false
    }

    def "can filter source files"() {
        given:
        buildFile << """
        tasks.named("licenseHeaders").configure {
            excludes << 'org/acme/filtered/**/*'
        }
        """
        dualLicensedFile()
        unknownSourceFile("src/main/java/org/acme/filtered/FilteredUnknownLicensed.java")
        unapprovedSourceFile("src/main/java/org/acme/filtered/FilteredUnapprovedLicensed.java")

        when:
        def result = gradleRunner("licenseHeaders").build()

        then:
        result.task(":licenseHeaders").outcome == TaskOutcome.SUCCESS
    }

    def "supports sspl by convention"() {
        given:
        dualLicensedFile()

        when:
        def result = gradleRunner("licenseHeaders").build()

        then:
        result.task(":licenseHeaders").outcome == TaskOutcome.SUCCESS
    }

    def "sspl default additional license can be overridden"() {
        given:
        buildFile << """
        tasks.named("licenseHeaders").configure {
            additionalLicense 'ELAST', 'Elastic License 2.0', '2.0; you may not use this file except in compliance with the Elastic License'
        }
        """
        elasticLicensed()
        dualLicensedFile()

        when:
        def result = gradleRunner("licenseHeaders").buildAndFail()

        then:
        result.task(":licenseHeaders").outcome == TaskOutcome.FAILED
    }

    private File unapprovedSourceFile(String filePath = "src/main/java/org/acme/UnapprovedLicensed.java") {
        File sourceFile = file(filePath);
        sourceFile << """
/*
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package ${packageString(sourceFile)};

 public class ${sourceFile.getName() - ".java"} {
 }
 """
    }

    private File unknownSourceFile(String filePath = "src/main/java/org/acme/UnknownLicensed.java") {
        File sourceFile = file(filePath);
        sourceFile << """
/*
 * Blubb my custom license shrug!
 */

 package ${packageString(sourceFile)};

 public class ${sourceFile.getName() - ".java"} {
 }
 """
    }

    private File dualLicensedFile() {
        file("src/main/java/org/acme/DualLicensed.java") << """
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

 package org.acme;
 public class DualLicensed {
 }
 """
    }

    private File elasticLicensed() {
        file("src/main/java/org/acme/ElasticLicensed.java") << """
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

 package org.acme;
 public class ElasticLicensed {
 }
 """
    }

    private String packageString(File sourceFile) {
        String normalizedPath = normalized(sourceFile.getPath())
        (normalizedPath.substring(normalizedPath.indexOf("src/main/java")) - "src/main/java/" - ("/" + sourceFile.getName())).replaceAll("/", ".")
    }

}
