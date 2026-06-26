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
import org.gradle.api.Project
import org.gradle.api.tasks.TaskContainer
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses

/**
 * Enforces compatibility with Gradle's <em>isolated projects</em> model, in which each project
 * is configured independently and cross-project access is forbidden at configuration time.
 *
 * <p>Each rule targets a specific {@link Project} API that crosses project boundaries or captures
 * mutable project state incompatible with project isolation. Every check is parameterised over
 * two class sets:
 * <ul>
 *   <li><b>java code</b> — Java/Groovy plugin classes compiled from {@code src/main};</li>
 *   <li><b>groovy scripts</b> — precompiled {@code *.gradle} script plugins compiled by Gradle's
 *       {@code groovy-gradle-plugin} into
 *       {@code build/groovy-dsl-plugins/output/plugin-classes/}.</li>
 * </ul>
 * Running each rule against both sets gives a distinct test entry per set, so CI failures
 * immediately identify whether a regression was introduced in Java code or in a script plugin.
 *
 * <p>Baseline sets are kept separate per class set ({@code KNOWN_*} for Java,
 * {@code KNOWN_*_IN_SCRIPTS} for scripts) so violations from the two worlds never obscure each
 * other. Each baseline is accompanied by a staleness test that fails once an entry is cleaned up,
 * ensuring baselines only ever shrink.
 */
class IsolatedProjectsArchUnitSpec extends AbstractArchUnitSpec {

    // -------------------------------------------------------------------------
    // Class sets under test
    // -------------------------------------------------------------------------

    @Shared
    JavaClasses productionClasses = importProductionClasses()

    @Shared
    JavaClasses scriptClasses = importScriptClasses()

    // -------------------------------------------------------------------------
    // Baselines — java code
    // -------------------------------------------------------------------------

    private static final Set<String> KNOWN_GET_ROOT_PROJECT = [
        "org.elasticsearch.gradle.internal.BaseInternalPluginBuildPlugin",
        "org.elasticsearch.gradle.internal.BuildPlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchBasePlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchJavaPlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin",
        "org.elasticsearch.gradle.internal.InternalAvailableTcpPortProviderPlugin",
        "org.elasticsearch.gradle.internal.InternalBwcGitPlugin",
        "org.elasticsearch.gradle.internal.InternalDistributionBwcSetupPlugin",
        "org.elasticsearch.gradle.internal.InternalDistributionDownloadPlugin",
        "org.elasticsearch.gradle.internal.InternalTestClustersPlugin",
        "org.elasticsearch.gradle.internal.MrjarPlugin",
        "org.elasticsearch.gradle.internal.docker.DockerSupportPlugin",
        "org.elasticsearch.gradle.internal.esql.EsqlFunctionPlugin",
        "org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin",
        "org.elasticsearch.gradle.internal.precommit.ForbiddenApisPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ThirdPartyAuditPrecommitPlugin",
        "org.elasticsearch.gradle.internal.snyk.SnykDependencyMonitoringGradlePlugin",
        "org.elasticsearch.gradle.internal.test.DistroTestPlugin",
        "org.elasticsearch.gradle.internal.test.InternalClusterTestPlugin",
        "org.elasticsearch.gradle.internal.test.MutedTestPlugin",
        "org.elasticsearch.gradle.internal.test.StandaloneRestTestPlugin",
        "org.elasticsearch.gradle.internal.test.StandaloneTestPlugin",
        "org.elasticsearch.gradle.internal.test.TestWithSslPlugin",
        "org.elasticsearch.gradle.internal.test.rest.RestTestBasePlugin",
        "org.elasticsearch.gradle.internal.test.rest.compat.compat.AbstractYamlRestCompatTestPlugin",
        "org.elasticsearch.gradle.internal.testfixtures.TestFixturesDeployPlugin",
        "org.elasticsearch.gradle.internal.transport.TransportVersionResourcesPlugin",
    ] as Set

    private static final Set<String> KNOWN_GET_ALL_SUB_PROJECTS = [
        "org.elasticsearch.gradle.internal.BaseInternalPluginBuildPlugin",
        "org.elasticsearch.gradle.internal.InternalDistributionBwcSetupPlugin",
        "org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditPrecommitPlugin",
        "org.elasticsearch.gradle.internal.test.rest.RestTestBasePlugin",
    ] as Set

    private static final Set<String> KNOWN_ALL_SUB_PROJECTS_CALLBACK = [] as Set

    private static final Set<String> KNOWN_EVALUATION_DEPENDS_ON = [
        "org.elasticsearch.gradle.internal.ElasticsearchJavadocPlugin",
    ] as Set

    private static final Set<String> KNOWN_GET_PARENT = [
        "org.elasticsearch.gradle.internal.InternalDistributionArchiveCheckPlugin",
    ] as Set

    // -------------------------------------------------------------------------
    // Baselines — groovy script plugins
    // -------------------------------------------------------------------------

    private static final Set<String> KNOWN_GET_ROOT_PROJECT_IN_SCRIPTS = [] as Set

    private static final Set<String> KNOWN_GET_ALL_SUB_PROJECTS_IN_SCRIPTS = [] as Set

    private static final Set<String> KNOWN_ALL_SUB_PROJECTS_CALLBACK_IN_SCRIPTS = [] as Set

    private static final Set<String> KNOWN_EVALUATION_DEPENDS_ON_IN_SCRIPTS = [] as Set

    private static final Set<String> KNOWN_GET_PARENT_IN_SCRIPTS = [] as Set

    // -------------------------------------------------------------------------
    // Rules
    // -------------------------------------------------------------------------

    def "production code in #label does not call Project.getRootProject()"() {
        given:
        ArchRule rule = noClasses()
            .that(notInBaseline(baseline))
            .should().callMethodWhere(projectMethodNamed("getRootProject"))
            .because("Project.getRootProject() navigates the live project hierarchy and is incompatible "
                + "with project isolation; pass the root-project directory as a task property, "
                + "build service, or ValueSource parameter instead")

        expect:
        rule.check(classes)

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_GET_ROOT_PROJECT
        "groovy scripts" | scriptClasses     | KNOWN_GET_ROOT_PROJECT_IN_SCRIPTS
    }

    def "production code in #label does not call Project.getAllprojects() or Project.getSubprojects()"() {
        given:
        ArchRule rule = noClasses()
            .that(notInBaseline(baseline))
            .should().callMethodWhere(projectMethodNamedAny("getAllprojects", "getSubprojects"))
            .because("Project.getAllprojects()/getSubprojects() return live collections of other project "
                + "instances and are incompatible with project isolation; use build services or "
                + "project registration events instead")

        expect:
        rule.check(classes)

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_GET_ALL_SUB_PROJECTS
        "groovy scripts" | scriptClasses     | KNOWN_GET_ALL_SUB_PROJECTS_IN_SCRIPTS
    }

    def "production code in #label does not register cross-project callbacks via Project.allprojects() or Project.subprojects()"() {
        given:
        ArchRule rule = noClasses()
            .that(notInBaseline(baseline))
            .should().callMethodWhere(projectMethodNamedAny("allprojects", "subprojects"))
            .because("Project.allprojects()/subprojects() register cross-project configuration callbacks "
                + "and are incompatible with project isolation; apply convention plugins in each project instead")

        expect:
        rule.check(classes)

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_ALL_SUB_PROJECTS_CALLBACK
        "groovy scripts" | scriptClasses     | KNOWN_ALL_SUB_PROJECTS_CALLBACK_IN_SCRIPTS
    }

    def "production code in #label does not call Project.evaluationDependsOn()"() {
        given:
        ArchRule rule = noClasses()
            .that(notInBaseline(baseline))
            .should().callMethodWhere(projectMethodNamed("evaluationDependsOn"))
            .because("Project.evaluationDependsOn() forces eager evaluation ordering between projects "
                + "and prevents Gradle from configuring projects in parallel")

        expect:
        rule.check(classes)

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_EVALUATION_DEPENDS_ON
        "groovy scripts" | scriptClasses     | KNOWN_EVALUATION_DEPENDS_ON_IN_SCRIPTS
    }

    def "production code in #label does not call Project.getParent()"() {
        given:
        ArchRule rule = noClasses()
            .that(notInBaseline(baseline))
            .should().callMethodWhere(projectMethodNamed("getParent"))
            .because("Project.getParent() navigates the live project hierarchy and is incompatible "
                + "with project isolation; pass required parent-project state as an explicit input instead")

        expect:
        rule.check(classes)

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_GET_PARENT
        "groovy scripts" | scriptClasses     | KNOWN_GET_PARENT_IN_SCRIPTS
    }

    def "production code in #label does not call Project.getProperties()"() {
        given:
        ArchRule rule = noClasses()
            .should().callMethodWhere(projectMethodNamed("getProperties"))
            .because("Project.getProperties() exposes the full mutable property map and is incompatible "
                + "with project isolation; access specific properties via project.findProperty() or providers instead")

        expect:
        rule.check(classes)

        where:
        label            | classes
        "java code"      | productionClasses
        "groovy scripts" | scriptClasses
    }

    def "production code in #label does not resolve tasks by cross-project path"() {
        given:
        ArchRule rule = noClasses()
            .should().callMethodWhere(taskContainerMethodNamedAny("findByPath", "getByPath"))
            .because("TaskContainer.findByPath()/getByPath() resolve tasks by cross-project path, "
                + "forcing the target project to be configured eagerly; use task dependencies "
                + "via project.dependencies or explicit task wiring instead")

        expect:
        rule.check(classes)

        where:
        label            | classes
        "java code"      | productionClasses
        "groovy scripts" | scriptClasses
    }

    // -------------------------------------------------------------------------
    // Staleness checks
    // -------------------------------------------------------------------------

    def "the #label getRootProject baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(baseline, classes) { JavaClass c ->
            callsProjectMethod(c, "getRootProject")
        }
        assert stale.isEmpty(), "Stale entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_GET_ROOT_PROJECT
        "groovy scripts" | scriptClasses     | KNOWN_GET_ROOT_PROJECT_IN_SCRIPTS
    }

    def "the #label getAllprojects/getSubprojects baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(baseline, classes) { JavaClass c ->
            callsProjectMethodAny(c, "getAllprojects", "getSubprojects")
        }
        assert stale.isEmpty(), "Stale entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_GET_ALL_SUB_PROJECTS
        "groovy scripts" | scriptClasses     | KNOWN_GET_ALL_SUB_PROJECTS_IN_SCRIPTS
    }

    def "the #label allprojects/subprojects callback baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(baseline, classes) { JavaClass c ->
            callsProjectMethodAny(c, "allprojects", "subprojects")
        }
        assert stale.isEmpty(), "Stale entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_ALL_SUB_PROJECTS_CALLBACK
        "groovy scripts" | scriptClasses     | KNOWN_ALL_SUB_PROJECTS_CALLBACK_IN_SCRIPTS
    }

    def "the #label evaluationDependsOn baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(baseline, classes) { JavaClass c ->
            callsProjectMethod(c, "evaluationDependsOn")
        }
        assert stale.isEmpty(), "Stale entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_EVALUATION_DEPENDS_ON
        "groovy scripts" | scriptClasses     | KNOWN_EVALUATION_DEPENDS_ON_IN_SCRIPTS
    }

    def "the #label getParent baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(baseline, classes) { JavaClass c ->
            callsProjectMethod(c, "getParent")
        }
        assert stale.isEmpty(), "Stale entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")

        where:
        label            | classes           | baseline
        "java code"      | productionClasses | KNOWN_GET_PARENT
        "groovy scripts" | scriptClasses     | KNOWN_GET_PARENT_IN_SCRIPTS
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static boolean callsProjectMethod(JavaClass clazz, String name) {
        return clazz.methodCallsFromSelf.any { call ->
            call.target.name == name && call.target.owner.isAssignableTo(Project)
        }
    }

    private static boolean callsProjectMethodAny(JavaClass clazz, String... names) {
        Set<String> nameSet = names as Set
        return clazz.methodCallsFromSelf.any { call ->
            nameSet.contains(call.target.name) && call.target.owner.isAssignableTo(Project)
        }
    }

    private static DescribedPredicate projectMethodNamed(String name) {
        return new DescribedPredicate("call Project.${name}()") {
            @Override
            boolean test(Object call) {
                return call.target.name == name && call.target.owner.isAssignableTo(Project)
            }
        }
    }

    private static DescribedPredicate projectMethodNamedAny(String... names) {
        Set<String> nameSet = names as Set
        String label = names.collect { "Project.${it}()" }.join(" or ")
        return new DescribedPredicate("call ${label}") {
            @Override
            boolean test(Object call) {
                return nameSet.contains(call.target.name) && call.target.owner.isAssignableTo(Project)
            }
        }
    }

    private static DescribedPredicate taskContainerMethodNamedAny(String... names) {
        Set<String> nameSet = names as Set
        String label = names.collect { "TaskContainer.${it}()" }.join(" or ")
        return new DescribedPredicate("call ${label}") {
            @Override
            boolean test(Object call) {
                return nameSet.contains(call.target.name) && call.target.owner.isAssignableTo(TaskContainer)
            }
        }
    }
}
