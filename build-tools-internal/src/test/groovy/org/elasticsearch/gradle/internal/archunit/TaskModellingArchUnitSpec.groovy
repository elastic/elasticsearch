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
import com.tngtech.archunit.core.domain.JavaModifier
import com.tngtech.archunit.lang.ArchCondition
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.ConditionEvents
import com.tngtech.archunit.lang.SimpleConditionEvent
import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.TaskContainer
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes

/**
 * Task-modelling conventions. Gradle generates managed property implementations (and validates
 * inputs/outputs) for task types that are {@code abstract} with abstract property getters, so task
 * types should be declared {@code abstract} rather than eagerly initialising their own fields.
 *
 * <p>Note: the complementary rule "every task property getter carries an input/output annotation"
 * is intentionally <em>not</em> implemented here — it is already enforced by Gradle's own
 * {@code validatePlugins} task (brought in by the {@code java-gradle-plugin}), which understands
 * task property semantics far better than a structural ArchUnit rule could.
 */
class TaskModellingArchUnitSpec extends AbstractArchUnitSpec {

    /**
     * Concrete (non-abstract) task types. New entries must not be added — declare new task types
     * {@code abstract} so Gradle can manage their properties. Existing entries should be removed as
     * they are made abstract (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_NON_ABSTRACT_TASKS = [
        "org.elasticsearch.gradle.internal.ConcatFilesTask",
        "org.elasticsearch.gradle.internal.EmptyDirTask",
        "org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask",
        "org.elasticsearch.gradle.internal.JavaClassPublicifier",
        "org.elasticsearch.gradle.internal.idea.EnablePreviewFeaturesTask",
        "org.elasticsearch.gradle.internal.precommit.JavaModulePrecommitTask",
        "org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditTask",
        "org.elasticsearch.gradle.internal.precommit.ValidateJsonAgainstSchemaTask",
        "org.elasticsearch.gradle.internal.precommit.ValidateJsonNoKeywordsTask",
        "org.elasticsearch.gradle.internal.precommit.ValidateYamlAgainstSchemaTask",
        "org.elasticsearch.gradle.internal.release.BundleChangelogsTask",
        "org.elasticsearch.gradle.internal.release.GenerateReleaseNotesTask",
        "org.elasticsearch.gradle.internal.release.PruneChangelogsTask",
        "org.elasticsearch.gradle.internal.release.TagVersionsTask",
        "org.elasticsearch.gradle.internal.release.UpdateBranchesJsonTask",
        "org.elasticsearch.gradle.internal.release.UpdateVersionsTask",
        "org.elasticsearch.gradle.internal.snyk.GenerateSnykDependencyGraph",
        "org.elasticsearch.gradle.internal.snyk.UploadSnykDependenciesGraph",
        "org.elasticsearch.gradle.internal.test.AntFixture",
        "org.elasticsearch.gradle.internal.test.rest.CopyRestApiTask",
    ] as Set

    /**
     * Classes that still create tasks eagerly via {@code TaskContainer.create(...)} (any overload)
     * or {@code Project.task(...)}. New entries must not be added — use the lazy
     * {@code register(...)} API instead. Existing entries should be removed as they are migrated
     * (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_EAGER_TASK_CREATION = [
    ] as Set

    /**
     * Classes that register an untyped task and define an inline {@code doLast}/{@code doFirst}
     * action on it. An untyped {@code register(String)} used purely as a lifecycle/aggregation task
     * is fine; once it carries action logic it should be a dedicated typed task class instead. New
     * entries must not be added; existing entries should be removed as they are migrated.
     *
     * <p>Detection is class-level: ArchUnit cannot prove the action is attached to the untyped task
     * specifically (no data-flow analysis), so a class that registers an untyped task and also uses
     * {@code doLast}/{@code doFirst} anywhere (including register/configure lambdas, which compile
     * to synthetic methods of the class) is flagged.
     */
    private static final Set<String> KNOWN_UNTYPED_REGISTER_WITH_ACTION = [
        "org.elasticsearch.gradle.internal.InternalDistributionBwcSetupPlugin",
        "org.elasticsearch.gradle.internal.precommit.CheckstylePrecommitPlugin",
    ] as Set

    @Shared
    JavaClasses productionClasses = importProductionClasses()

    def "build logic does not create tasks eagerly"() {
        given:
        ArchRule rule = classes()
            .that(notInBaseline(KNOWN_EAGER_TASK_CREATION))
            .should(notEagerlyCreateTasks())
            .because("TaskContainer.create()/Project.task() create tasks eagerly; use the lazy register(...) API instead")

        expect:
        rule.check(productionClasses)
    }

    def "the eager-task-creation baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_EAGER_TASK_CREATION, productionClasses) { JavaClass c ->
            eagerlyCreatesTask(c)
        }
        assert stale.isEmpty(), "Stale KNOWN_EAGER_TASK_CREATION entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")
    }

    def "untyped registered tasks do not define inline actions"() {
        given:
        ArchRule rule = classes()
            .that(notInBaseline(KNOWN_UNTYPED_REGISTER_WITH_ACTION))
            .should(notRegisterUntypedTaskWithInlineAction())
            .because("an untyped register(String) task that defines doLast/doFirst should be a dedicated typed task class")

        expect:
        rule.check(productionClasses)
    }

    def "the untyped-register-with-action baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_UNTYPED_REGISTER_WITH_ACTION, productionClasses) { JavaClass c ->
            registersUntypedTask(c) && definesInlineAction(c)
        }
        assert stale.isEmpty(), "Stale KNOWN_UNTYPED_REGISTER_WITH_ACTION entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")
    }

    def "task types are abstract"() {
        given:
        ArchRule rule = classes()
            .that().areAssignableTo(DefaultTask)
            .and().areTopLevelClasses()
            .and().areNotInterfaces()
            .and(notInBaseline(KNOWN_NON_ABSTRACT_TASKS))
            .should().haveModifier(JavaModifier.ABSTRACT)
            .because("Gradle task types should be abstract so Gradle can generate managed property implementations")

        expect:
        rule.check(productionClasses)
    }

    def "the non-abstract-tasks baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_NON_ABSTRACT_TASKS, productionClasses) { JavaClass c ->
            c.isAssignableTo(DefaultTask) && c.modifiers.contains(JavaModifier.ABSTRACT) == false
        }
        assert stale.isEmpty(), "Stale KNOWN_NON_ABSTRACT_TASKS entries (made abstract or removed) — delete them:\n  " + stale.join("\n  ")
    }

    private static ArchCondition<JavaClass> notEagerlyCreateTasks() {
        return new ArchCondition<JavaClass>("not create tasks eagerly") {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                if (eagerlyCreatesTask(item)) {
                    events.add(SimpleConditionEvent.violated(item, "${item.fullName} creates a task eagerly via create()/task()"))
                }
            }
        }
    }

    private static ArchCondition<JavaClass> notRegisterUntypedTaskWithInlineAction() {
        return new ArchCondition<JavaClass>("not register an untyped task with an inline doLast/doFirst action") {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                if (registersUntypedTask(item) && definesInlineAction(item)) {
                    events.add(SimpleConditionEvent.violated(item,
                        "${item.fullName} registers an untyped task and defines an inline doLast/doFirst action"))
                }
            }
        }
    }

    /** True if the class eagerly creates a task: {@code TaskContainer.create(...)} or {@code Project.task(...)}. */
    private static boolean eagerlyCreatesTask(JavaClass clazz) {
        return clazz.methodCallsFromSelf.any { call ->
            def target = call.target
            if (target.name == "create" && isTaskContainer(target.owner)) {
                return true
            }
            if (target.name == "task" && target.owner.isAssignableTo(Project)) {
                return true
            }
            return false
        }
    }

    /** True if the class registers an untyped task via the no-type {@code register(String)} overload. */
    private static boolean registersUntypedTask(JavaClass clazz) {
        return clazz.methodCallsFromSelf.any { call ->
            def target = call.target
            List params = target.rawParameterTypes
            boolean singleStringArg = params.size() == 1 && params[0].fullName == "java.lang.String"
            return target.name == "register" && singleStringArg && isTaskContainer(target.owner)
        }
    }

    /** True if the class defines a task action via {@code Task.doLast}/{@code doFirst}. */
    private static boolean definesInlineAction(JavaClass clazz) {
        return clazz.methodCallsFromSelf.any { call ->
            def target = call.target
            return (target.name == "doLast" || target.name == "doFirst") && target.owner.isAssignableTo(Task)
        }
    }

    private static boolean isTaskContainer(JavaClass owner) {
        return owner.isAssignableTo(TaskContainer) || owner.fullName == TaskContainer.name
    }
}
