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
import com.tngtech.archunit.core.domain.JavaModifier
import com.tngtech.archunit.lang.ArchRule
import org.gradle.api.Plugin
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses

/**
 * Structural and lifecycle conventions for the build logic:
 * <ul>
 *   <li>classes named {@code *Plugin} actually implement {@link Plugin} (and vice versa), so plugin
 *       discovery and naming stay consistent;</li>
 *   <li>no use of {@code Project#afterEvaluate}, a deferred-configuration hook that is fragile,
 *       order-dependent and discouraged in favour of lazy configuration.</li>
 * </ul>
 */
class GradlePluginConventionsArchUnitSpec extends AbstractArchUnitSpec {

    /**
     * Classes that still call {@code afterEvaluate}. New entries must not be added — prefer lazy
     * configuration (providers, {@code configureEach}) instead. Existing entries should be removed
     * as they are migrated (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_AFTER_EVALUATE = [
        "org.elasticsearch.gradle.internal.BaseInternalPluginBuildPlugin",
    ] as Set

    @Shared
    JavaClasses productionClasses = importProductionClasses()

    def "classes named *Plugin implement Plugin"() {
        given:
        ArchRule rule = classes()
            .that().haveSimpleNameEndingWith("Plugin")
            .and().areTopLevelClasses()
            .and().areNotInterfaces()
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .should().beAssignableTo(Plugin)
            .because("the *Plugin suffix is reserved for Gradle Plugin implementations")

        expect:
        rule.check(productionClasses)
    }

    def "Plugin implementations are named *Plugin"() {
        given:
        ArchRule rule = classes()
            .that().areAssignableTo(Plugin)
            .and().areTopLevelClasses()
            .and().areNotInterfaces()
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .should().haveSimpleNameEndingWith("Plugin")
            .because("Gradle plugins should be discoverable by the *Plugin naming convention")

        expect:
        rule.check(productionClasses)
    }

    def "production code does not use Project.afterEvaluate"() {
        given:
        ArchRule rule = noClasses()
            .that(notInBaseline(KNOWN_AFTER_EVALUATE))
            .should().callMethodWhere(targetNamed("afterEvaluate"))
            .because("afterEvaluate defers configuration and is order-dependent; use lazy configuration instead")

        expect:
        rule.check(productionClasses)
    }

    def "the afterEvaluate baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_AFTER_EVALUATE, productionClasses) { JavaClass c ->
            c.methodCallsFromSelf.any { it.target.name == "afterEvaluate" }
        }
        assert stale.isEmpty(), "Stale KNOWN_AFTER_EVALUATE entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")
    }

    private static DescribedPredicate targetNamed(String name) {
        return new DescribedPredicate("call ${name}()") {
            @Override
            boolean test(Object call) {
                return call.target.name == name
            }
        }
    }
}
