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
import com.tngtech.archunit.lang.ArchRule
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses

/**
 * Enforces that production build logic logs through the Gradle {@code Logger} rather than the JVM
 * console streams. Direct {@code System.out}/{@code System.err} access and
 * {@code Throwable#printStackTrace()} bypass Gradle's logging configuration (levels, capture,
 * console formatting) and produce output that cannot be filtered or attributed.
 */
class LoggingArchUnitSpec extends AbstractArchUnitSpec {

    /**
     * Production classes that still write to the console directly. New entries must not be added —
     * use {@code org.gradle.api.logging.Logging.getLogger(...)} instead. Existing entries should be
     * removed as they are migrated (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_CONSOLE_LOGGING = [
        "org.elasticsearch.gradle.internal.ElasticsearchBuildCompletePlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin",
        "org.elasticsearch.gradle.internal.InternalDistributionArchiveCheckPlugin",
        "org.elasticsearch.gradle.internal.dependencies.patches.azurecore.AzureCoreClassPatcher",
        "org.elasticsearch.gradle.internal.dependencies.patches.hdfs.HdfsClassPatcher",
        "org.elasticsearch.gradle.internal.doc.DocsTestPlugin",
        "org.elasticsearch.gradle.internal.packer.CacheCacheableTestFixtures",
        "org.elasticsearch.gradle.internal.test.ErrorReportingTestListener",
    ] as Set

    @Shared
    JavaClasses productionClasses = importProductionClasses()

    def "production code does not log to System.out, System.err or printStackTrace"() {
        given:
        ArchRule rule = noClasses()
            .that(notInBaseline(KNOWN_CONSOLE_LOGGING))
            .should().accessField(System.class, "out")
            .orShould().accessField(System.class, "err")
            .orShould().callMethodWhere(targetMethodNamed("printStackTrace"))
            .because("use the Gradle Logger (org.gradle.api.logging) instead of console streams or printStackTrace")

        expect:
        rule.check(productionClasses)
    }

    def "the console-logging baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_CONSOLE_LOGGING, productionClasses) { JavaClass c ->
            logsToConsole(c)
        }
        assert stale.isEmpty(), "Stale KNOWN_CONSOLE_LOGGING entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")
    }

    private static com.tngtech.archunit.base.DescribedPredicate targetMethodNamed(String name) {
        return new com.tngtech.archunit.base.DescribedPredicate("call ${name}()") {
            @Override
            boolean test(Object call) {
                return call.target.name == name
            }
        }
    }

    private static boolean logsToConsole(JavaClass clazz) {
        boolean field = clazz.fieldAccessesFromSelf.any {
            it.target.owner.isEquivalentTo(System.class) && (it.target.name == "out" || it.target.name == "err")
        }
        boolean call = clazz.methodCallsFromSelf.any { it.target.name == "printStackTrace" }
        return field || call
    }
}
