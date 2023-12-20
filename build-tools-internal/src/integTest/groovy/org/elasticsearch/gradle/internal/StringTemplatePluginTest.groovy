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

    def "test substitution"() {
        given:
        internalBuild()
        file('src/main/p/X-Box.java.st') << """
          public class \$Type\$Box {
            final \$type\$ value;
            public \$Type\$Box(\$type\$ value) {
              this.value = value;
            }
          }
        """.stripIndent().stripTrailing()
        file('src/main/generated-src/someFile.txt') << """
          Just some random data
        """

        buildFile << """
          apply plugin: 'elasticsearch.build'
          apply plugin: 'elasticsearch.string-templates'

          tasks.named("stringTemplates").configure {
            template {
              it.properties = ["Type" : "Int", "type" : "int"]
              it.inputFile = file("src/main/p/X-Box.java.st")
              it.outputFile = "p/IntBox.java"
            }
            template {
              it.properties = ["Type" : "Long", "type" : "long"]
              it.inputFile = file("src/main/p/X-Box.java.st")
              it.outputFile = "p/LongBox.java"
            }
          }
        """

        when:
        def result = gradleRunner("stringTemplates", '-g', gradleUserHome).build()

        then:
        result.task(":stringTemplates").outcome == TaskOutcome.SUCCESS
        file("src/main/generated-src/someFile.txt").exists() == false
        file("src/main/generated-src/p/IntBox.java").exists()
        file("src/main/generated-src/p/LongBox.java").exists()

        //assert output
        normalized(file("src/main/generated-src/p/IntBox.java").text) == """
            public class IntBox {
              final int value;
              public IntBox(int value) {
                this.value = value;
              }
            }
          """.stripIndent().stripTrailing()
        normalized(file("src/main/generated-src/p/LongBox.java").text) == """
            public class LongBox {
              final long value;
              public LongBox(long value) {
                this.value = value;
              }
            }
          """.stripIndent().stripTrailing()
    }

    def "test basic conditional"() {
        given:
        internalBuild()
        file('src/main/Color.txt.st') << """
          \$if(Red)\$1 Red\$endif\$
          \$if(Blue)\$2 Blue\$endif\$
          \$if(Green)\$3 Green\$endif\$
        """.stripIndent().stripTrailing()

        buildFile << """
          apply plugin: 'elasticsearch.build'
          apply plugin: 'elasticsearch.string-templates'

          tasks.named("stringTemplates").configure {
            template {
              it.properties = ["Red" : "true", "Blue" : "", "Green" : "true"]
              it.inputFile = file("src/main/Color.txt.st")
              it.outputFile = "Color.txt"
            }
          }
        """

        when:
        def result = gradleRunner("stringTemplates", '-g', gradleUserHome).build()

        then:
        result.task(":stringTemplates").outcome == TaskOutcome.SUCCESS
        file("src/main/generated-src/Color.txt").exists()
        //assert output
        normalized(file("src/main/generated-src/Color.txt").text) == """
            1 Red
            3 Green
          """.stripIndent().stripTrailing()
    }

    def "test if then else"() {
        given:
        internalBuild()
        file('src/main/Token.txt.st') << """
          \$if(Foo)\$1 Foo
          \$elseif(Bar)\$2 Bar
          \$else\$3 Baz
          \$endif\$
        """.stripIndent().stripTrailing()

        buildFile << """
          apply plugin: 'elasticsearch.build'
          apply plugin: 'elasticsearch.string-templates'

          tasks.named("stringTemplates").configure {
            template {
              it.properties = [:] // no properties
              it.inputFile = file("src/main/Token.txt.st")
              it.outputFile = "Token.txt"
            }
          }
        """

        when:
        def result = gradleRunner("stringTemplates", '-g', gradleUserHome).build()

        then:
        result.task(":stringTemplates").outcome == TaskOutcome.SUCCESS
        file("src/main/generated-src/Token.txt").exists()
        //assert output
        normalized(file("src/main/generated-src/Token.txt").text) == """
            3 Baz
          """.stripIndent().stripTrailing()
    }

    def "output file already present and up to date"() {
        given:
        internalBuild()
        file('src/main/UpToDate.txt.st') << """
          Hello World!
        """.stripIndent().stripTrailing()
        // the output file is already created and up to date (content wise)
        file('src/main/generated-src/UpToDate.txt') << """
          Hello World!
        """.stripIndent().stripTrailing()

        buildFile << """
          apply plugin: 'elasticsearch.build'
          apply plugin: 'elasticsearch.string-templates'

          tasks.named("stringTemplates").configure {
            template {
              it.properties = [:] // no properties
              it.inputFile = file("src/main/UpToDate.txt.st")
              it.outputFile = "UpToDate.txt"
            }
          }
        """

        when:
        def result = gradleRunner("stringTemplates", '-g', gradleUserHome).build()

        then:
        result.task(":stringTemplates").outcome == TaskOutcome.SUCCESS
        file("src/main/generated-src/UpToDate.txt").exists()
        //assert output - expect the original output
        normalized(file("src/main/generated-src/UpToDate.txt").text) == """
            Hello World!
          """.stripIndent().stripTrailing()
    }

    def "output file already present but not up to date"() {
        given:
        internalBuild()
        file('src/main/Message.txt.st') << """
          Hello World!
        """.stripIndent().stripTrailing()
        // the output file is already created, but not up to date (content wise)
        file('src/main/generated-src/Message.txt') << """
          Hello Chris xx
        """.stripIndent().stripTrailing()

        buildFile << """
          apply plugin: 'elasticsearch.build'
          apply plugin: 'elasticsearch.string-templates'

          tasks.named("stringTemplates").configure {
            template {
              it.properties = [:] // no properties
              it.inputFile = file("src/main/Message.txt.st")
              it.outputFile = "Message.txt"
            }
          }
        """

        when:
        def result = gradleRunner("stringTemplates", '-g', gradleUserHome).build()

        then:
        result.task(":stringTemplates").outcome == TaskOutcome.SUCCESS
        file("src/main/generated-src/Message.txt").exists()
        //assert output - expect the updated message output
        normalized(file("src/main/generated-src/Message.txt").text) == """
            Hello World!
          """.stripIndent().stripTrailing()
    }

    def "cleanup delete files"() {
        given:
        internalBuild()
        file('src/main/generated-src/someFile.txt') << """
          Just some random data
        """

        buildFile << """
          apply plugin: 'elasticsearch.build'
          apply plugin: 'elasticsearch.string-templates'

          tasks.named("stringTemplates").configure {
            // no templates
          }
        """

        when:
        def result = gradleRunner("stringTemplates", '-g', gradleUserHome).build()

        then:
        result.task(":stringTemplates").outcome == TaskOutcome.SUCCESS
        file("src/main/generated-src/someFile.txt").exists() == false
        file("src/main/generated-src").exists() == true
    }
}
