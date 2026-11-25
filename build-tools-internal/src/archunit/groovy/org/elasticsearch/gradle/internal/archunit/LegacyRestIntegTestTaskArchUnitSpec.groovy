/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.archunit

import com.tngtech.archunit.base.DescribedPredicate
import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.lang.ArchRule
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses

/**
 * Forbids new usages of the legacy REST integ test task types in the compiled build logic of {@code build-tools},
 * {@code build-tools-internal} and {@code build-conventions}.
 *
 * <p>{@code StandaloneRestIntegTestTask} and its internal subtype {@code RestIntegTestTask} belong to the Gradle
 * test-clusters framework, which is being replaced by JUnit rule based test clusters. New REST integ test tasks must be
 * plain {@code Test} tasks registered via {@code RestIntegTestsExtension.register(...)} (or
 * {@code RestTestUtil.registerPlainRestTestTask(...)}).
 *
 * <p>ArchUnit only sees compiled references. Build scripts and dynamic Groovy usages are covered by the
 * {@code legacy-rest-integ-test-task} rule of {@code validateBuildGradleScripts}.
 */
class LegacyRestIntegTestTaskArchUnitSpec extends AbstractArchUnitSpec {

    /** The legacy task types themselves. They define the types, so they are not usages. */
    private static final Set<String> LEGACY_TYPES = [
        "org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask",
        "org.elasticsearch.gradle.internal.test.RestIntegTestTask",
    ] as Set

    /**
     * Classes that still depend on a legacy type. New entries must not be added. Existing entries should be removed as
     * they are migrated (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_LEGACY_USAGES = [
        // the build-tools REST test plugins for external plugin authors
        "org.elasticsearch.gradle.test.JavaRestTestPlugin",
        "org.elasticsearch.gradle.test.YamlRestTestPlugin",
        // the legacy wiring itself, removed together with the types
        "org.elasticsearch.gradle.internal.test.LegacyRestTestBasePlugin",
        // treats legacy tasks as REST integ tests until all projects are migrated
        "org.elasticsearch.gradle.internal.test.rest.RestIntegTestsExtension",
    ] as Set

    @Shared
    JavaClasses buildLogicClasses = importAllBuildLogicProductionClasses()

    def "imports the legacy types"() {
        expect:
        // guards against the importer silently matching nothing, which would make the rules below pass trivially
        LEGACY_TYPES.every { buildLogicClasses.contain(it) }
    }

    def "build logic does not use the legacy REST integ test task types"() {
        given:
        ArchRule rule = noClasses()
            .that(notALegacyType())
            .and(notInBaseline(KNOWN_LEGACY_USAGES))
            .should().dependOnClassesThat(isLegacyType())
            .because("the legacy test-clusters framework is being removed; register a plain Test task via " +
                "restTests.register(...) and use JUnit rule based test clusters instead")

        expect:
        rule.check(buildLogicClasses)
    }

    def "the legacy usage baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_LEGACY_USAGES, buildLogicClasses) { JavaClass c -> usesLegacyType(c) }
        assert stale.isEmpty(), "Stale KNOWN_LEGACY_USAGES entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")
    }

    private static boolean usesLegacyType(JavaClass javaClass) {
        return javaClass.directDependenciesFromSelf.any { LEGACY_TYPES.contains(it.targetClass.fullName) }
    }

    private static DescribedPredicate<JavaClass> isLegacyType() {
        return new DescribedPredicate<JavaClass>("are legacy REST integ test task types") {
            @Override
            boolean test(JavaClass javaClass) {
                return LEGACY_TYPES.contains(javaClass.fullName)
            }
        }
    }

    private static DescribedPredicate<JavaClass> notALegacyType() {
        return new DescribedPredicate<JavaClass>("are not legacy REST integ test task types") {
            @Override
            boolean test(JavaClass javaClass) {
                return LEGACY_TYPES.contains(topLevelName(javaClass)) == false
            }
        }
    }
}
