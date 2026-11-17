/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.archunit

import com.tngtech.archunit.core.domain.JavaAccess
import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.domain.JavaMethodCall
import com.tngtech.archunit.lang.ArchCondition
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.ConditionEvents
import com.tngtech.archunit.lang.SimpleConditionEvent
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses

/**
 * Enforces that the compiled build logic in {@code build-tools}, {@code build-tools-internal} and
 * {@code build-conventions} does not use Ant.
 *
 * <p>Gradle 9.9 deprecated its Ant integration and Gradle 10 removes it, including the Ant classes on the
 * build script and plugin classpath. Use Gradle APIs ({@code Copy}/{@code Sync}, {@code FileSystemOperations})
 * or the copy filters in {@code org.elasticsearch.gradle.internal.conventions.filters} instead.
 *
 * <p>ArchUnit only sees statically compiled references. Dynamic Groovy calls such as {@code ant.move(...)} and
 * all {@code .gradle} scripts are covered by the {@code ant-usage} rule of {@code validateBuildGradleScripts}.
 */
class AntUsageArchUnitSpec extends AbstractArchUnitSpec {

    /** Gradle API members that are part of the deprecated Ant integration. */
    private static final Set<String> GRADLE_ANT_METHODS = ["getAnt", "ant", "createAntBuilder", "addToAntBuilder"] as Set

    /** Gradle API types that are part of the deprecated Ant integration. */
    private static final Set<String> GRADLE_ANT_TYPES = [
        "org.gradle.api.AntBuilder",
        "org.gradle.api.tasks.ant.AntTarget",
        "org.gradle.api.tasks.AntBuilderAware",
        'org.gradle.api.file.FileCollection$AntType',
    ] as Set

    @Shared
    JavaClasses buildLogicClasses = importAllBuildLogicProductionClasses()

    def "imports the classes of all build logic builds"() {
        expect:
        // guards against the importer silently matching nothing, which would make the rules below pass trivially
        ["org.elasticsearch.gradle.internal.conventions.GUtils",
         "org.elasticsearch.gradle.Version",
         "org.elasticsearch.gradle.internal.BuildPlugin"].every { buildLogicClasses.contain(it) }
    }

    def "build logic does not depend on Ant classes"() {
        given:
        ArchRule rule = noClasses()
            .should().dependOnClassesThat().resideInAnyPackage("org.apache.tools.ant..", "groovy.ant..")
            .because("Gradle 10 removes Ant from the build classpath; use Gradle APIs or the copy filters in " +
                "org.elasticsearch.gradle.internal.conventions.filters instead")

        expect:
        rule.check(buildLogicClasses)
    }

    def "build logic does not use the Gradle Ant integration"() {
        given:
        ArchRule rule = classes()
            .should(notUseGradleAntIntegration())
            .because("the Gradle Ant integration is deprecated since Gradle 9.9 and removed in Gradle 10")

        expect:
        rule.check(buildLogicClasses)
    }

    private static ArchCondition<JavaClass> notUseGradleAntIntegration() {
        return new ArchCondition<JavaClass>("not use the Gradle Ant integration") {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                item.directDependenciesFromSelf
                    .findAll { GRADLE_ANT_TYPES.contains(it.targetClass.fullName) }
                    .each { events.add(SimpleConditionEvent.violated(item, it.description)) }
                item.methodCallsFromSelf
                    .findAll { isGradleAntMethodCall(it) }
                    .each { events.add(SimpleConditionEvent.violated(item, it.description)) }
            }
        }
    }

    private static boolean isGradleAntMethodCall(JavaMethodCall call) {
        return GRADLE_ANT_METHODS.contains(call.name) && call.targetOwner.packageName.startsWith("org.gradle.")
    }
}
