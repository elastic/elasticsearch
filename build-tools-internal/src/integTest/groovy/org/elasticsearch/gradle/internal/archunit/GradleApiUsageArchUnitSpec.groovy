/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.archunit

import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.lang.ArchCondition
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.ConditionEvents
import com.tngtech.archunit.lang.SimpleConditionEvent
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes

/**
 * Enforces that the production build logic in {@code build-tools-internal} does not depend on
 * unstable Gradle internal APIs (any package with an {@code internal} segment under
 * {@code org.gradle}). Those APIs carry no compatibility guarantees and routinely break across
 * Gradle upgrades, so new usages should always be replaced with the public API.
 *
 * <p>Existing usages are recorded in {@link #KNOWN_INTERNAL_API_USAGES} (keyed on the top-level
 * class) as a baseline so the rule passes today while failing for any <em>new</em> usage. The
 * baseline is intended to only shrink: the staleness test fails if an entry no longer exists or has
 * already been cleaned up, forcing it to be removed.
 *
 * <p>Note on mechanism: ArchUnit's {@code FreezingArchRule} could host this baseline, but its text
 * store records each violation with a source line number, so unrelated edits churn the committed
 * store. A class-name baseline is line-number independent, reviewable in the diff and consistent
 * with the other architecture specs, so it is preferred here.
 */
class GradleApiUsageArchUnitSpec extends AbstractArchUnitSpec {

    /**
     * Production classes that still depend on Gradle internal APIs. New entries must not be added —
     * use the public Gradle API instead. Existing entries should be removed as the usages are
     * migrated (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_INTERNAL_API_USAGES = [
        "org.elasticsearch.gradle.internal.AntFixtureStop",
        "org.elasticsearch.gradle.internal.DependenciesInfoTask",
        "org.elasticsearch.gradle.internal.ElasticsearchBuildCompletePlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin",
        "org.elasticsearch.gradle.internal.EmptyDirTask",
        "org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask",
        "org.elasticsearch.gradle.internal.InternalDistributionBwcSetupPlugin",
        "org.elasticsearch.gradle.internal.JarApiComparisonTask",
        "org.elasticsearch.gradle.internal.NoticeTask",
        "org.elasticsearch.gradle.internal.ResolveAllDependencies",
        "org.elasticsearch.gradle.internal.SymbolicLinkPreservingTar",
        "org.elasticsearch.gradle.internal.doc.DocsTestPlugin",
        "org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin",
        "org.elasticsearch.gradle.internal.precommit.CheckstylePrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ForbiddenApisPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ForbiddenPatternsTask",
        "org.elasticsearch.gradle.internal.release.ReleaseToolsPlugin",
        "org.elasticsearch.gradle.internal.test.DistroTestPlugin",
        "org.elasticsearch.gradle.internal.test.ErrorReportingTestListener",
        "org.elasticsearch.gradle.internal.test.LegacyRestTestBasePlugin",
        "org.elasticsearch.gradle.internal.test.rest.CopyRestApiTask",
        "org.elasticsearch.gradle.internal.test.rest.CopyRestTestsTask",
        "org.elasticsearch.gradle.internal.test.rest.RestTestBasePlugin",
        "org.elasticsearch.gradle.internal.test.rest.compat.compat.AbstractYamlRestCompatTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.compat.compat.LegacyYamlRestCompatTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.compat.compat.RestCompatTestTransformTask",
        "org.elasticsearch.gradle.internal.test.rest.compat.compat.YamlRestCompatTestPlugin",
        "org.elasticsearch.gradle.internal.util.ports.AvailablePortAllocator",
    ] as Set

    @Shared
    JavaClasses productionClasses = importProductionClasses()

    def "production code does not depend on Gradle internal APIs"() {
        given:
        ArchRule rule = classes()
            .that(notInBaseline(KNOWN_INTERNAL_API_USAGES))
            .should(notDependOnGradleInternalApi())
            .because("Gradle internal APIs are unstable across versions; use the public Gradle API instead")

        expect:
        rule.check(productionClasses)
    }

    def "the internal-API baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_INTERNAL_API_USAGES, productionClasses) { JavaClass c ->
            dependsOnGradleInternalApi(c)
        }
        assert stale.isEmpty(), "Stale KNOWN_INTERNAL_API_USAGES entries (cleaned up or removed) — delete them:\n  " + stale.join("\n  ")
    }

    private static ArchCondition<JavaClass> notDependOnGradleInternalApi() {
        return new ArchCondition<JavaClass>("not depend on Gradle internal APIs") {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                if (dependsOnGradleInternalApi(item)) {
                    events.add(SimpleConditionEvent.violated(item, "${item.fullName} depends on a Gradle internal API"))
                }
            }
        }
    }

    private static boolean dependsOnGradleInternalApi(JavaClass clazz) {
        return clazz.directDependenciesFromSelf.any { dependency ->
            String pkg = dependency.targetClass.packageName
            return pkg.startsWith("org.gradle") && (pkg.contains(".internal.") || pkg.endsWith(".internal"))
        }
    }
}
