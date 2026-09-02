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
import org.gradle.api.Task
import spock.lang.Shared

import org.gradle.api.Project
import org.gradle.api.invocation.Gradle
import org.gradle.api.artifacts.Configuration

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields

/**
 * Configuration-cache hygiene for task implementations. A {@link Task} that calls
 * {@code getProject()} captures the live {@code Project} model, which is unsupported with the
 * configuration cache — the same class of problem as referencing {@code project}/{@code logger}
 * from an execution-time closure. Task logic should instead take its inputs as managed properties
 * and use injected services ({@code ObjectFactory}, {@code ProjectLayout}, {@code ExecOperations}, …).
 */
class ConfigurationCacheArchUnitSpec extends AbstractArchUnitSpec {

    /**
     * Task types that still call {@code getProject()}. New entries must not be added — inject the
     * required services or model the value as a task property instead. Existing entries should be
     * removed as they are made configuration-cache safe (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_GET_PROJECT_IN_TASKS = [
    ] as Set

    @Shared
    JavaClasses productionClasses = importProductionClasses()

    def "task implementations do not call getProject()"() {
        given:
        ArchRule rule = noClasses()
            .that().areAssignableTo(Task)
            .and(notInBaseline(KNOWN_GET_PROJECT_IN_TASKS))
            .should().callMethodWhere(targetNamed("getProject"))
            .because("Task.getProject() captures the Project model and breaks the configuration cache; "
                + "use task properties and injected services instead")

        expect:
        rule.check(productionClasses)
    }

    def "task implementations do not hold Project, Gradle or Configuration fields"() {
        given:
        ArchRule rule = noFields()
            .that().areDeclaredInClassesThat().areAssignableTo(Task)
            .should().haveRawType(Project)
            .orShould().haveRawType(Gradle)
            .orShould().haveRawType(Configuration)
            .because("capturing the Project/Gradle model or a Configuration as task state breaks the configuration cache")

        expect:
        rule.check(productionClasses)
    }

    def "the getProject-in-tasks baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_GET_PROJECT_IN_TASKS, productionClasses) { JavaClass c ->
            c.isAssignableTo(Task) && c.methodCallsFromSelf.any { it.target.name == "getProject" }
        }
        assert stale.isEmpty(), "Stale KNOWN_GET_PROJECT_IN_TASKS entries (fixed or removed) — delete them:\n  " + stale.join("\n  ")
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
