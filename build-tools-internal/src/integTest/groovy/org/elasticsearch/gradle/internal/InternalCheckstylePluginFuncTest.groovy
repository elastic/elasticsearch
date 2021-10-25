/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.TaskOutcome

class InternalCheckstylePluginFuncTest extends AbstractGradleFuncTest {

    def "compatibility options are resolved from from build params minimum runtime version"() {
        given:
        checkstyleConfig()
        file('src/main/java/org/acme/Acme.java') << """
        package org.acme;
        
        public class Acme {
            static void test() {
                if(!false) {
                    System.out.println("should not happen.");
                }
            }
        }
        """

        buildFile.text = """
        plugins {
          id 'java'
          id 'elasticsearch.internal-checkstyle'
        }
        
        repositories {
            mavenCentral()
        }
        """

        when:
        def result = gradleRunner("checkstyleMain", "-i", "--stacktrace").buildAndFail()

        then:
        result.getOutput().contains("Checkstyle has encountered 1 problem(s).")
        normalized(file("build/reports/checkstyle/checkstyleMain.xml").text) == """<?xml version="1.0" encoding="UTF-8"?>
<checkstyle version="8.37">
<file name="./src/main/java/org/acme/Acme.java">
<error line="6" column="20" severity="error" message="Do not negate boolean expressions with !, but check explicitly with == false as it is more explicit" source="BooleanNegation"/>
</file>
</checkstyle>"""
        result.task(":checkstyleMain").outcome == TaskOutcome.FAILED;
    }

    private File checkstyleConfig() {
        def configFile = file("config/checkstyle/checkstyle.xml")
        configFile << """<?xml version="1.0"?>
<!DOCTYPE module PUBLIC
    "-//Puppy Crawl//DTD Check Configuration 1.3//EN"
    "http://www.puppycrawl.com/dtds/configuration_1_3.dtd">

<module name="Checker">
  <property name="charset" value="UTF-8" />

  <module name="SuppressWarningsFilter" />

  <!-- Checks Java files and forbids empty Javadoc comments. -->
  <!-- Although you can use the "JavadocStyle" rule for this, it considers Javadoc -->
  <!-- that only contains a "@return" line to be empty. -->
  <module name="RegexpMultiline">
    <property name="id" value="EmptyJavadoc" />
    <property name="format" value="\\/\\*[\\s\\*]*\\*\\/" />
    <property name="fileExtensions" value="java" />
    <property name="message" value="Empty javadoc comments are forbidden" />
  </module>

  <!--
    We include snippets that are wrapped in `// tag` and `// end` into the
    docs, stripping the leading spaces. If the context is wider than 76
    characters then it'll need to scroll. This fails the build if it sees
    such snippets.
  -->
  <module name="org.elasticsearch.gradle.internal.checkstyle.SnippetLengthCheck">
    <property name="id" value="SnippetLength" />
    <property name="max" value="76" />
  </module>

  <!-- Its our official line length! See checkstyle_suppressions.xml for the files that don't pass this. For now we
    suppress the check there but enforce it everywhere else. This prevents the list from getting longer even if it is
    unfair. -->
  <module name="LineLength">
    <property name="max" value="140" />
    <property name="ignorePattern" value="^ *\\* *https?://[^ ]+\$" />
  </module>

  <module name="TreeWalker">
    <!-- Make the @SuppressWarnings annotations available to Checkstyle -->
    <module name="SuppressWarningsHolder" />

    <module name="AvoidStarImport" />

    <!-- Unused imports are forbidden -->
    <module name="UnusedImports" />

    <!-- Non-inner classes must be in files that match their names. -->
    <module name="OuterTypeFilename" />

    <!-- No line wraps inside of import and package statements. -->
    <module name="NoLineWrap" />

    <!-- only one statement per line should be allowed -->
    <module name="OneStatementPerLine" />

    <!-- Each java file has only one outer class -->
    <module name="OneTopLevelClass" />


    <!-- Forbid using '!' for logical negations in favour of checking against 'false' explicitly. -->
    <!-- This is only reported in the IDE for now because there are many violations -->
    <module name="DescendantToken">
        <property name="id" value="BooleanNegation" />
        <property name="tokens" value="EXPR"/>
        <property name="limitedTokens" value="LNOT"/>
        <property name="maximumNumber" value="0"/>
        <message
            key="descendant.token.max"
            value="Do not negate boolean expressions with '!', but check explicitly with '== false' as it is more explicit"/>
    </module>

  </module>
</module>
        """

        return configFile
    }
}