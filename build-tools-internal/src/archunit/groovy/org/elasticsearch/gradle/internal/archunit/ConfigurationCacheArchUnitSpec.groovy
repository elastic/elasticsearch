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
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.ArchCondition
import com.tngtech.archunit.lang.ConditionEvents
import com.tngtech.archunit.lang.SimpleConditionEvent
import org.elasticsearch.gradle.internal.fixtures.ExecutionActionFixture
import org.gradle.api.Task
import spock.lang.Shared

import org.gradle.api.Project
import org.gradle.api.invocation.Gradle
import org.gradle.api.artifacts.Configuration

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes
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

    // Exact capture counts, not blanket exclusions of Action implementations. Only direct
    // arguments at Task execution-registration calls are inspected (see the bytecode helper).
    // Measured on main ff3ed1ce5a62: no direct Project/Gradle captures in this narrow pattern.
    private static final Map<String, Integer> KNOWN_EXECUTION_MODEL_CAPTURES = [:]

    private static ArchRule executionCaptureRule(Map<String, Integer> baseline) {
        return classes().should(new ArchCondition<JavaClass>("not capture live models in execution actions") {
            @Override
            void check(JavaClass type, ConditionEvents events) {
                def findings = ExecutionActionModelCapture.scan(type.fullName, getClass().classLoader)
                int expected = baseline.getOrDefault(type.fullName, 0)
                findings.drop(expected).each { finding ->
                    events.add(SimpleConditionEvent.violated(type,
                        "${finding.owner()}.${finding.method()}:${finding.line()} ${finding.action()} captures ${finding.capture()}"))
                }
            }
        }).because("execution actions cannot retain Project/Gradle; capture values or use task inputs/services instead")

    }

    def "execution actions do not directly capture Project or Gradle"() {
        expect:
        executionCaptureRule(KNOWN_EXECUTION_MODEL_CAPTURES).check(productionClasses)
    }

    private static List<String> staleExecutionCaptures(Map<String, Integer> baseline, JavaClasses types) {
        return baseline.findAll { name, expected ->
            types.contain(name) == false || ExecutionActionModelCapture.scan(name, ConfigurationCacheArchUnitSpec.classLoader).size() != expected
        }.keySet().sort()
    }

    def "the execution capture baseline has exact counts and no stale entries"() {
        expect:
        staleExecutionCaptures(KNOWN_EXECUTION_MODEL_CAPTURES, productionClasses).isEmpty()
    }

    def "execution capture rule and baseline reject regressions and stale counts"() {
        given:
        def fixture = new ClassFileImporter().importClasses(ExecutionActionFixture)
        String name = ExecutionActionFixture.name

        expect:
        executionCaptureRule([:]).evaluate(fixture).hasViolation()
        executionCaptureRule([(name): 3]).evaluate(fixture).hasViolation()
        executionCaptureRule([(name): 4]).evaluate(fixture).hasViolation() == false
        staleExecutionCaptures([(name): 4], fixture).isEmpty()
        staleExecutionCaptures([(name): 5], fixture) == [name]
        staleExecutionCaptures(['removed.Fixture': 1], fixture) == ['removed.Fixture']
    }

    def "bytecode probe identifies execution captures but not configuration actions or unrelated receivers"() {
        when:
        def findings = ExecutionActionModelCapture.scan(ExecutionActionFixture)

        then:
        findings.size() == 4
        findings.every { it.method() == 'unsafe' && it.line() > 0 }
        findings*.action() == ['doFirst', 'doLast', 'onlyIf', 'doLast']
        findings*.capture() == [Project.name, Gradle.name, Project.name, Project.name]
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
