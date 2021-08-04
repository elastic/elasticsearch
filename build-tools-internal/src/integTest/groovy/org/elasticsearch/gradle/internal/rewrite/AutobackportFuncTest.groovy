/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite;

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest;

class AutobackportFuncTest extends AbstractGradleFuncTest {

    def "can run rewrite"() {
        when:
        setupRewriteYamlConfig()


        def sourceFile = file("src/main/java/org/acme/SomeClass.java")
        sourceFile << """
package org.acme;

import java.util.List;
import java.util.Map;
import java.util.Set;

class SomeClass {
    public void someMethod() {
        List myList = List.of("some", "non", "java8", "code");
        Set mySet = Set.of("some", "non", "java8", "code");
        Map myMap = Map.of(List.of("some", "non"), Set.of("java8", "code"));
    }
}
"""

        buildFile.text = """
        plugins {
          id 'java'
          id 'elasticsearch.rewrite'
        }
        def rewriteVersionString = "7.10.0"
        rewrite {
            rewriteVersion = rewriteVersionString
            activeRecipe("org.elasticsearch.java.backport.ListOfBackport",
                    "org.elasticsearch.java.backport.MapOfBackport",
                    "org.elasticsearch.java.backport.SetOfBackport")
            configFile = rootProject.file("rewrite.yml")
        }
        
        repositories {
            mavenCentral()
        }
        
        configurations {
            rewriteRun {
                resolutionStrategy {
                    force 'org.jetbrains:annotations:21.0.1'
                    force 'org.slf4j:slf4j-api:1.7.31'
                    force 'org.jetbrains.kotlin:kotlin-stdlib:1.5.10'
                    force 'org.jetbrains.kotlin:kotlin-stdlib-common:1.5.10'
                }
            }
            rewriteDryRun {
                resolutionStrategy {
                    force 'org.jetbrains:annotations:21.0.1'
                    force 'org.slf4j:slf4j-api:1.7.31'
                    force 'org.jetbrains.kotlin:kotlin-stdlib:1.5.10'
                    force 'org.jetbrains.kotlin:kotlin-stdlib-common:1.5.10'
                }
            }
            rewrite {
                extendsFrom rewriteRun
                resolutionStrategy {
                    force 'org.jetbrains:annotations:21.0.1'
                    force 'org.slf4j:slf4j-api:1.7.31'
                    force 'org.jetbrains.kotlin:kotlin-stdlib:1.5.10'
                    force 'org.jetbrains.kotlin:kotlin-stdlib-common:1.5.10'
                }
            }
        }
        
        dependencies {
            rewriteRun "org.openrewrite:rewrite-java-11:" + rewriteVersionString
            rewriteRun "org.openrewrite:rewrite-java-8:" + rewriteVersionString
            rewriteRun "org.openrewrite:rewrite-maven:" + rewriteVersionString
            rewriteRun "org.openrewrite:rewrite-xml:" + rewriteVersionString
            rewriteRun "org.openrewrite:rewrite-yaml:" + rewriteVersionString
            rewriteRun "org.openrewrite:rewrite-properties:" + rewriteVersionString
        }
        """

        then:
        gradleRunner("build", "rewriteRun", "-i").build()

        sourceFile.text == """
package org.acme;

import org.elasticsearch.core.Lists;
import org.elasticsearch.core.Maps;
import org.elasticsearch.core.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

class SomeClass {
    public void someMethod() {
        List myList = Lists.of("some", "non", "java8", "code");
        Set mySet = Sets.of("some", "non", "java8", "code");
        Map myMap = Maps.of(Lists.of("some", "non"), Sets.of("java8", "code"));
    }
}
"""
    }

    private File setupRewriteYamlConfig() {
        file("rewrite.yml") << """
type: specs.openrewrite.org/v1beta/recipe
name: org.elasticsearch.java.backport.ListOfBackport
displayName: Use `org.elasticsearch.core.Lists#of(..)` not java.util.List.of#(..)
description: Java 8 does not support the `java.util.List#of(..)`.
tags:
  - jdk
recipeList:
  - org.openrewrite.java.ChangeMethodTargetToStatic:
      methodPattern: java.util.List of(..)
      fullyQualifiedTargetTypeName: org.elasticsearch.core.Lists
---
type: specs.openrewrite.org/v1beta/recipe
name: org.elasticsearch.java.backport.MapOfBackport
displayName: Use `org.elasticsearch.core.Maps#of(..)` not java.util.Map.of#(..)
description: Java 8 does not support the `java.util.Map#of(..)`.
tags:
  - jdk
recipeList:
  - org.openrewrite.java.ChangeMethodTargetToStatic:
      methodPattern: java.util.Map of(..)
      fullyQualifiedTargetTypeName: org.elasticsearch.core.Maps
---
type: specs.openrewrite.org/v1beta/recipe
name: org.elasticsearch.java.backport.SetOfBackport
displayName: Use `org.elasticsearch.core.Sets#of(..)` not java.util.Set.of#(..)
description: Java 8 does not support the `java.util.Set#of(..)`.
tags:
  - jdk
recipeList:
  - org.openrewrite.java.ChangeMethodTargetToStatic:
      methodPattern: java.util.Set of(..)
      fullyQualifiedTargetTypeName: org.elasticsearch.core.Sets
"""
    }
}
