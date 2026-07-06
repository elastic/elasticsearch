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
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.lang.ArchCondition
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.ConditionEvents
import com.tngtech.archunit.lang.SimpleConditionEvent
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.api.Task
import spock.lang.Shared
import spock.lang.Specification

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes

/**
 * Architecture rules that enforce a minimum level of test coverage for the build logic in
 * {@code build-tools-internal}.
 *
 * <p>Rather than measuring line coverage, these rules assert that every production
 * <em>plugin</em> and <em>task</em> is paired, by naming convention, with a dedicated test
 * class. This guards against new build logic landing without any test at all.
 *
 * <ul>
 *   <li><b>Plugins</b> ({@link Plugin} implementations) must have a {@code *FuncTest} that extends
 *       {@link AbstractGradleInternalPluginFuncTest}, exercising them against a real Gradle build
 *       via TestKit.</li>
 *   <li><b>Tasks</b> ({@link Task} implementations) must have a corresponding test, either a
 *       unit test ({@code *Tests}/{@code *Test}/{@code *Spec}) or a {@code *FuncTest}/{@code *IT}.</li>
 * </ul>
 *
 * <p><b>Baseline allowlist.</b> The build logic predates these rules, so a number of existing
 * plugins and tasks are not yet covered. Those are recorded in {@link #KNOWN_UNCOVERED} so the
 * rules pass today while still failing for any <em>newly</em> added plugin or task. The allowlist
 * is intended to only ever shrink: the {@code allowlist contains no stale entries} test fails if
 * an entry no longer exists or has since gained a test, forcing the entry to be removed.
 *
 * <p><b>Class discovery.</b> Plugin and task subjects are imported from the runtime classpath
 * with ArchUnit, so the type hierarchy ({@code assignableTo Plugin/Task}) is resolved from real
 * bytecode. The set of existing test classes is gathered by scanning the {@code test} and
 * {@code integTest} source trees of this module directly — the {@code test} source set is not on
 * the integTest classpath, so a filesystem scan is the simplest self-contained way to index it.
 * No extra build wiring is required: everything needed lives under {@code src/integTest}.
 */
class IntegTestCoverageArchUnitSpec extends Specification {

    /** Test class name suffixes accepted as coverage for a task. */
    private static final List<String> TEST_SUFFIXES = ["Tests", "Test", "Spec", "FuncTest", "IT"]

    /**
     * Plugins and tasks that predate these coverage rules and are not yet tested. New entries must
     * not be added here — add a test instead. Existing entries should be removed as coverage is
     * filled in (the staleness test enforces this).
     */
    private static final Set<String> KNOWN_UNCOVERED = [
        // --- plugins lacking a *FuncTest ---
        "org.elasticsearch.gradle.internal.BaseInternalPluginBuildPlugin",
        "org.elasticsearch.gradle.internal.DependenciesInfoPlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchBasePlugin",
        "org.elasticsearch.gradle.internal.EmbeddedProviderPlugin",
        "org.elasticsearch.gradle.internal.InternalAvailableTcpPortProviderPlugin",
        "org.elasticsearch.gradle.internal.InternalPluginBuildPlugin",
        "org.elasticsearch.gradle.internal.InternalReaperPlugin",
        "org.elasticsearch.gradle.internal.InternalTestArtifactBasePlugin",
        "org.elasticsearch.gradle.internal.InternalTestArtifactPlugin",
        "org.elasticsearch.gradle.internal.InternalTestClustersPlugin",
        "org.elasticsearch.gradle.internal.MrjarPlugin",
        "org.elasticsearch.gradle.internal.ProjectSubscribeServicePlugin",
        "org.elasticsearch.gradle.internal.RepositoriesSetupPlugin",
        "org.elasticsearch.gradle.internal.dependencies.rules.ComponentMetadataRulesPlugin",
        "org.elasticsearch.gradle.internal.docker.DockerSupportPlugin",
        "org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin",
        "org.elasticsearch.gradle.internal.packer.CacheTestFixtureResourcesPlugin",
        "org.elasticsearch.gradle.internal.precommit.DependencyLicensesPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.FilePermissionsPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ForbiddenApisPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ForbiddenPatternsPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.JarHellPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.JavaModulePrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.LoggerUsagePrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ValidateRestSpecPlugin",
        "org.elasticsearch.gradle.internal.release.ReleaseToolsPlugin",
        "org.elasticsearch.gradle.internal.test.ClusterFeaturesMetadataPlugin",
        "org.elasticsearch.gradle.internal.test.DistroTestPlugin",
        "org.elasticsearch.gradle.internal.test.InternalClusterTestPlugin",
        "org.elasticsearch.gradle.internal.test.LegacyRestTestBasePlugin",
        "org.elasticsearch.gradle.internal.test.MutedTestPlugin",
        "org.elasticsearch.gradle.internal.test.StandaloneRestTestPlugin",
        "org.elasticsearch.gradle.internal.test.StandaloneTestPlugin",
        "org.elasticsearch.gradle.internal.test.TestWithDependenciesPlugin",
        "org.elasticsearch.gradle.internal.test.TestWithSslPlugin",
        "org.elasticsearch.gradle.internal.test.rest.InternalJavaRestTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.InternalYamlRestTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.LegacyJavaRestTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.RestTestBasePlugin",
        "org.elasticsearch.gradle.internal.test.rest.compat.compat.YamlRestCompatTestPlugin",
        "org.elasticsearch.gradle.internal.testfixtures.TestFixturesDeployPlugin",
        "org.elasticsearch.gradle.internal.transport.TransportVersionReferencesPlugin",
        "org.elasticsearch.gradle.internal.transport.TransportVersionResourcesPlugin",

        // --- plugins with a *FuncTest that cannot extend AbstractGradleInternalPluginFuncTest ---
        // These plugins require setup before they are applied (bwcVersions resolvable at apply
        // time, the java plugin applied first, or application in subprojects rather than the root),
        // which conflicts with the base's apply-on-setup behaviour. Their existing func tests keep
        // their original base class until the harness supports deferred/multi-project application.
        "org.elasticsearch.gradle.internal.InternalDistributionArchiveCheckPlugin",
        "org.elasticsearch.gradle.internal.InternalDistributionDownloadPlugin",
        "org.elasticsearch.gradle.internal.doc.DocsTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.LegacyYamlRestTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.RestResourcesPlugin",
        "org.elasticsearch.gradle.internal.test.rest.compat.compat.LegacyYamlRestCompatTestPlugin",

        // --- tasks lacking a unit or functional test ---
        "org.elasticsearch.gradle.internal.JavaClassPublicifier",
        "org.elasticsearch.gradle.internal.idea.EnablePreviewFeaturesTask",
        "org.elasticsearch.gradle.internal.precommit.JavaModulePrecommitTask",
        "org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditTask",
        "org.elasticsearch.gradle.internal.precommit.ValidateJsonAgainstSchemaTask",
        "org.elasticsearch.gradle.internal.precommit.ValidateJsonNoKeywordsTask",
        "org.elasticsearch.gradle.internal.precommit.ValidateYamlAgainstSchemaTask",
        "org.elasticsearch.gradle.internal.release.BundleChangelogsTask",
        "org.elasticsearch.gradle.internal.snyk.GenerateSnykDependencyGraph",
        "org.elasticsearch.gradle.internal.snyk.UploadSnykDependenciesGraph",
        "org.elasticsearch.gradle.internal.test.AntFixture",
        "org.elasticsearch.gradle.internal.test.rest.CopyRestApiTask",
    ] as Set

    /** Production + integTest classes, imported from the runtime classpath. */
    @Shared
    JavaClasses productionClasses = new ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
        .importPackages("org.elasticsearch.gradle")

    /**
     * Simple names of every test class across the {@code test} and {@code integTest} source
     * sets, used to look up whether a {@code <Subject><suffix>} test class exists.
     */
    @Shared
    Set<String> testClassNames = discoverTestClassNames()

    /**
     * Simple names of every func test that extends {@link AbstractGradleInternalPluginFuncTest}.
     * A plugin is only considered covered when its {@code <Plugin>FuncTest} appears here — i.e. it
     * not only exists by name but is actually wired up against the internal-plugin func test harness.
     */
    @Shared
    Set<String> pluginFuncTestNames = productionClasses
        .findAll { it.isAssignableTo(AbstractGradleInternalPluginFuncTest) }
        .collect { it.simpleName } as Set

    def "every Gradle plugin is covered by a FuncTest extending AbstractGradleInternalPluginFuncTest"() {
        given:
        ArchRule rule = classes()
            .that().areAssignableTo(Plugin)
            .and().areTopLevelClasses()
            .and().areNotInterfaces()
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .and().resideOutsideOfPackage("org.elasticsearch.gradle.fixtures..")
            .should(beCoveredByAPluginFuncTest())
            .because("every Gradle plugin must be covered by a *FuncTest that extends "
                + AbstractGradleInternalPluginFuncTest.name)

        expect:
        rule.check(productionClasses)
    }

    def "every Gradle task is covered by a test"() {
        given:
        ArchRule rule = classes()
            .that().areAssignableTo(Task)
            .and().areTopLevelClasses()
            .and().areNotInterfaces()
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .should(haveACorrespondingTestNamed(TEST_SUFFIXES as String[]))
            .because("every Gradle task must be covered by a unit test or functional test")

        expect:
        rule.check(productionClasses)
    }

    def "the known-uncovered allowlist contains no stale entries"() {
        given: "production classes indexed by fully qualified name"
        Map<String, JavaClass> byName = productionClasses.collectEntries { [(it.fullName): it] }

        when: "an allowlisted class no longer exists, or has since gained the required coverage"
        List<String> stale = KNOWN_UNCOVERED.findAll { String fqn ->
            JavaClass clazz = byName[fqn]
            return clazz == null || isCovered(clazz)
        }.sort()

        then: "the entry must be removed from KNOWN_UNCOVERED"
        String message = "Stale KNOWN_UNCOVERED entries (now tested or removed) — delete them:\n  " + stale.join("\n  ")
        assert stale.isEmpty(), message
    }

    /**
     * Whether a subject is covered: plugins need a {@code <Plugin>FuncTest} extending
     * {@link AbstractGradleInternalPluginFuncTest}, every other subject (tasks) needs any test
     * class matching {@link #TEST_SUFFIXES}.
     */
    private boolean isCovered(JavaClass clazz) {
        if (clazz.isAssignableTo(Plugin)) {
            return pluginFuncTestNames.contains(clazz.simpleName + "FuncTest")
        }
        return TEST_SUFFIXES.any { testClassNames.contains(clazz.simpleName + it) }
    }

    /**
     * Builds a condition satisfied when a plugin {@code Foo} has a {@code FooFuncTest} that extends
     * {@link AbstractGradleInternalPluginFuncTest}. Classes listed in {@link #KNOWN_UNCOVERED} are
     * treated as an accepted baseline gap and never reported.
     */
    private ArchCondition<JavaClass> beCoveredByAPluginFuncTest() {
        String base = AbstractGradleInternalPluginFuncTest.simpleName
        return new ArchCondition<JavaClass>("be covered by a *FuncTest extending ${base}") {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                if (KNOWN_UNCOVERED.contains(item.fullName)) {
                    return // accepted baseline gap, see KNOWN_UNCOVERED
                }
                String expected = item.simpleName + "FuncTest"
                if (pluginFuncTestNames.contains(expected) == false) {
                    events.add(SimpleConditionEvent.violated(
                        item,
                        "${item.fullName} has no ${expected} extending ${base}"
                    ))
                }
            }
        }
    }

    /**
     * Builds a condition that is satisfied when, for a class {@code Foo}, at least one of the
     * candidate test class names {@code Foo<suffix>} exists across the source sets. Classes listed
     * in {@link #KNOWN_UNCOVERED} are treated as an accepted baseline gap and never reported.
     */
    private ArchCondition<JavaClass> haveACorrespondingTestNamed(String... suffixes) {
        String description = "have a corresponding " + suffixes.collect { "*" + it }.join(" or ") + " test class"
        return new ArchCondition<JavaClass>(description) {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                if (KNOWN_UNCOVERED.contains(item.fullName)) {
                    return // accepted baseline gap, see KNOWN_UNCOVERED
                }
                List<String> candidates = suffixes.collect { item.simpleName + it }
                boolean covered = candidates.any { testClassNames.contains(it) }
                if (covered == false) {
                    events.add(SimpleConditionEvent.violated(
                        item,
                        "${item.fullName} has no test class (expected one of: ${candidates.join(', ')})"
                    ))
                }
            }
        }
    }

    /**
     * Scans the {@code test} and {@code integTest} source roots of this module for Java/Groovy
     * sources and returns their simple class names (file base names). The Gradle {@code Test}
     * task runs with the module directory as its working directory, so the source roots are
     * resolved relative to {@code user.dir}.
     */
    private static Set<String> discoverTestClassNames() {
        Set<String> names = new HashSet<>()
        for (File sourceRoot : testSourceRoots()) {
            sourceRoot.eachFileRecurse { File f ->
                if (f.isFile() && (f.name.endsWith(".java") || f.name.endsWith(".groovy"))) {
                    names.add(f.name.substring(0, f.name.lastIndexOf('.')))
                }
            }
        }
        return names
    }

    private static List<File> testSourceRoots() {
        File moduleDir = new File(System.getProperty("user.dir"))
        List<File> roots = ["src/test", "src/integTest"].collect { new File(moduleDir, it) }.findAll { it.isDirectory() }
        if (roots.isEmpty()) {
            // Fallback for runners (e.g. IDEs) whose working dir is the repository root.
            File module = new File(moduleDir, "build-tools-internal")
            roots = ["src/test", "src/integTest"].collect { new File(module, it) }.findAll { it.isDirectory() }
        }
        assert roots.isEmpty() == false : "Could not locate build-tools-internal test sources relative to ${moduleDir}"
        return roots
    }
}
