/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.domain.JavaModifier
import com.tngtech.archunit.lang.ArchCondition
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.ConditionEvents
import com.tngtech.archunit.lang.SimpleConditionEvent
import org.gradle.api.DefaultTask
import org.gradle.api.Project
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
     * Classes that still create untyped (plain {@code DefaultTask}) tasks via the no-type
     * {@code register(String)} / {@code create(String)} / {@code Project.task(...)} overloads. New
     * entries must not be added — register tasks with a dedicated typed task class instead.
     * Existing entries should be removed as they are migrated (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_UNTYPED_TASK_CREATORS = [
        "org.elasticsearch.gradle.internal.EmbeddedProviderPlugin",
        "org.elasticsearch.gradle.internal.InternalDistributionBwcSetupPlugin",
        "org.elasticsearch.gradle.internal.precommit.CheckstylePrecommitPlugin",
        "org.elasticsearch.gradle.internal.test.DistroTestPlugin",
    ] as Set

    @Shared
    JavaClasses productionClasses = importProductionClasses()

    def "build logic does not create untyped (DefaultTask) tasks"() {
        given:
        ArchRule rule = classes()
            .that(notInBaseline(KNOWN_UNTYPED_TASK_CREATORS))
            .should(notCreateUntypedTasks())
            .because("untyped DefaultTask tasks have no managed inputs/outputs; register tasks with a dedicated typed task class")

        expect:
        rule.check(productionClasses)
    }

    def "the untyped-task-creators baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_UNTYPED_TASK_CREATORS, productionClasses) { JavaClass c ->
            createsUntypedTask(c)
        }
        assert stale.isEmpty(), "Stale KNOWN_UNTYPED_TASK_CREATORS entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")
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

    private static ArchCondition<JavaClass> notCreateUntypedTasks() {
        return new ArchCondition<JavaClass>("not create untyped (DefaultTask) tasks") {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                if (createsUntypedTask(item)) {
                    events.add(SimpleConditionEvent.violated(item, "${item.fullName} creates an untyped DefaultTask"))
                }
            }
        }
    }

    /**
     * True if the class registers/creates a task without a task type: the no-type
     * {@code register(String)} / {@code create(String)} / {@code create(Map)} overloads on a task
     * container, or any {@code Project.task(...)} overload. These all yield a plain
     * {@code DefaultTask}.
     */
    private static boolean createsUntypedTask(JavaClass clazz) {
        return clazz.methodCallsFromSelf.any { call ->
            def target = call.target
            String name = target.name
            List params = target.rawParameterTypes
            boolean singleStringArg = params.size() == 1 && params[0].fullName == "java.lang.String"
            if ((name == "register" || name == "create") && singleStringArg && isTaskContainer(target.owner)) {
                return true
            }
            if (name == "create" && params.size() == 1 && params[0].isAssignableTo(Map) && isTaskContainer(target.owner)) {
                return true
            }
            if (name == "task" && target.owner.isAssignableTo(Project)) {
                return true
            }
            return false
        }
    }

    private static boolean isTaskContainer(JavaClass owner) {
        return owner.isAssignableTo(TaskContainer) || owner.fullName == TaskContainer.name
    }
}
