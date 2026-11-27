/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.archunit

import com.tngtech.archunit.core.domain.Dependency
import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaClasses
import spock.lang.Shared

/**
 * Forbids new usages of the legacy REST integ test task types in the compiled build logic of {@code build-tools},
 * {@code build-tools-internal} and {@code build-conventions}.
 *
 * <p>{@code StandaloneRestIntegTestTask} and its internal subtype {@code RestIntegTestTask} belong to the Gradle
 * test-clusters framework, which is being replaced by JUnit rule based test clusters. New REST integ test tasks must be
 * plain {@code Test} tasks registered via {@code RestIntegTestsExtension.register(...)} (or
 * {@code RestTestUtil.registerPlainRestTestTask(...)}).
 *
 * <p>The baseline counts the existing legacy dependencies per top-level class (nested classes and lambdas included)
 * instead of exempting whole classes, so a new usage fails even inside a class that is already baselined. This mirrors
 * the count based baseline of the {@code legacy-rest-integ-test-task} rule of {@code validateBuildGradleScripts}, which
 * covers build scripts and dynamic Groovy usages that ArchUnit cannot see.
 */
class LegacyRestIntegTestTaskArchUnitSpec extends AbstractArchUnitSpec {

    /** The legacy task types themselves. They define the types, so they are not usages. */
    private static final Set<String> LEGACY_TYPES = [
        "org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask",
        "org.elasticsearch.gradle.internal.test.RestIntegTestTask",
    ] as Set

    /**
     * Number of legacy type dependencies each class still has. New entries must not be added and counts must not grow.
     * Lower or remove an entry when usages are migrated (the staleness test enforces this).
     */
    private static final Map<String, Integer> KNOWN_LEGACY_USAGES = [
        // the build-tools REST test plugins for external plugin authors
        "org.elasticsearch.gradle.test.JavaRestTestPlugin": 6,
        "org.elasticsearch.gradle.test.YamlRestTestPlugin": 7,
        // the legacy wiring itself, removed together with the types
        "org.elasticsearch.gradle.internal.test.LegacyRestTestBasePlugin": 17,
        // treats legacy tasks as REST integ tests until all projects are migrated
        "org.elasticsearch.gradle.internal.test.rest.RestIntegTestsExtension": 1,
        // still used by the Legacy*RestTestPlugin classes on this branch
        "org.elasticsearch.gradle.internal.test.rest.RestTestUtil": 3,
        // configures SSL for legacy test-clusters based tasks
        "org.elasticsearch.gradle.internal.test.TestWithSslPlugin": 4,
    ]

    @Shared
    JavaClasses buildLogicClasses = importAllBuildLogicProductionClasses()

    def "imports the legacy types"() {
        expect:
        // guards against the importer silently matching nothing, which would make the checks below pass trivially
        LEGACY_TYPES.every { buildLogicClasses.contain(it) }
    }

    def "build logic does not add usages of the legacy REST integ test task types"() {
        given:
        Map<String, List<Dependency>> usages = legacyUsages(buildLogicClasses)

        when:
        List<String> violations = usages.findAll { String owner, List<Dependency> deps ->
            deps.size() > KNOWN_LEGACY_USAGES.getOrDefault(owner, 0)
        }.collect { String owner, List<Dependency> deps ->
            "${owner}: ${deps.size()} usages, baseline ${KNOWN_LEGACY_USAGES.getOrDefault(owner, 0)}\n    " +
                deps*.description.sort().join("\n    ")
        }.sort()

        then:
        assert violations.isEmpty(), "New usages of the legacy REST integ test task types. The legacy test-clusters " +
            "framework is being removed; register a plain Test task via restTests.register(...) and use JUnit rule " +
            "based test clusters instead:\n  " + violations.join("\n  ")
    }

    def "the legacy usage baseline contains no stale entries"() {
        given:
        Map<String, List<Dependency>> usages = legacyUsages(buildLogicClasses)

        when:
        List<String> stale = KNOWN_LEGACY_USAGES.findAll { String owner, Integer count ->
            (usages[owner]?.size() ?: 0) < count
        }.collect { String owner, Integer count ->
            "${owner}: baseline ${count}, actual ${usages[owner]?.size() ?: 0}"
        }.sort()

        then:
        assert stale.isEmpty(), "Stale KNOWN_LEGACY_USAGES entries (migrated or removed) — lower or delete them:\n  " +
            stale.join("\n  ")
    }

    /** Legacy type dependencies grouped by the top-level class they originate from, excluding the legacy types. */
    private static Map<String, List<Dependency>> legacyUsages(JavaClasses classes) {
        Map<String, List<Dependency>> usages = [:]
        classes.each { JavaClass javaClass ->
            String owner = topLevelName(javaClass)
            if (LEGACY_TYPES.contains(owner)) {
                return
            }
            javaClass.directDependenciesFromSelf.findAll { LEGACY_TYPES.contains(it.targetClass.fullName) }.each {
                usages.computeIfAbsent(owner) { [] } << it
            }
        }
        return usages
    }
}
