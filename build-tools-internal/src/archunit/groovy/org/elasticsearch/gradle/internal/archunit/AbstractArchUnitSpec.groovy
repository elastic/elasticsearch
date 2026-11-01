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
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import spock.lang.Specification

/**
 * Shared infrastructure for the ArchUnit specs that enforce Gradle best practices on the build
 * logic in {@code build-tools-internal}.
 *
 * <p>Provides class importers and a small helper for the baseline pattern these specs share: a
 * curated {@code Set} of fully qualified names that are exempt from a rule, plus a staleness check
 * that fails once an exempt class is removed or no longer violates — so every baseline can only
 * shrink.
 */
abstract class AbstractArchUnitSpec extends Specification {

    /**
     * Imports production classes only — those compiled from {@code src/main} (java or precompiled
     * groovy plugins), excluding test sources and dependency jars. This is the right scope for
     * best-practice rules, which target the build logic we ship rather than test fixtures.
     */
    protected static JavaClasses importProductionClasses() {
        return new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
            .withImportOption({ location ->
                location.contains("/classes/java/main/") || location.contains("/classes/groovy/main/")
            } as ImportOption)
            .importPackages("org.elasticsearch.gradle")
    }

    /**
     * Returns the baseline entries that are no longer valid — either no class with that top-level
     * name exists any more, or none of them (top-level or nested) still violates the rule
     * ({@code stillViolates} returns false for all). Baselines key on the top-level class (see
     * {@link #notInBaseline}), so staleness considers nested classes and closures too. A non-empty
     * result must fail the owning spec so the baseline is kept honest and only ever shrinks.
     */
    protected static List<String> staleBaselineEntries(
        Set<String> baseline,
        JavaClasses classes,
        Closure<Boolean> stillViolates
    ) {
        Map<String, List<JavaClass>> byTopLevel = classes.toList().groupBy { topLevelName(it) }
        return baseline.findAll { String fqn ->
            List<JavaClass> group = byTopLevel[fqn]
            return group == null || group.every { stillViolates.call(it) == false }
        }.sort()
    }

    /** A predicate selecting classes whose top-level enclosing class is not in {@code baseline}. */
    protected static DescribedPredicate<JavaClass> notInBaseline(Set<String> baseline) {
        return new DescribedPredicate<JavaClass>("not an accepted baseline class") {
            @Override
            boolean test(JavaClass javaClass) {
                return baseline.contains(topLevelName(javaClass)) == false
            }
        }
    }

    /** The fully qualified name of the outermost enclosing class (inner classes collapse onto it). */
    protected static String topLevelName(JavaClass javaClass) {
        return javaClass.fullName.split('\\$')[0]
    }
}
