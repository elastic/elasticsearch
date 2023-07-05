/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest;
import org.gradle.testkit.runner.TaskOutcome;

class StringTemplatePluginTest extends AbstractGradleFuncTest {

    def "test simple substitution"() {
        given:
        internalBuild()
        file('src/main/p/X-Box.java.st') << """
          public class \$Type\$Box {
            final \$type\$ value;
            public Code(\$type\$ value) {
              this.value = value;
            }
          }
        """

        buildFile << """
          apply plugin: 'elasticsearch.build'
          apply plugin: 'elasticsearch.string-templates'

          tasks.named("stringTemplates").configure {
            template {
              it.properties = ["Type" : "Int", "type" : "int"]
              it.inputFile = new File("${projectDir}/src/main/p/X-Box.java.st")
              it.outputFile = "p/IntBox.java"
            }
            template {
              it.properties = ["Type" : "Long", "type" : "long"]
              it.inputFile = new File("${projectDir}/src/main/p/X-Box.java.st")
              it.outputFile = "p/LongBox.java"
            }
          }
        """

        when:
        def result = gradleRunner("stringTemplates", '-g', gradleUserHome).build()

        then:
        result.task(":stringTemplates").outcome == TaskOutcome.SUCCESS
        file("src/main/generated-src/p/IntBox.java").exists()
        file("src/main/generated-src/p/LongBox.java").exists()
        //TODO: assert output
    }
}
