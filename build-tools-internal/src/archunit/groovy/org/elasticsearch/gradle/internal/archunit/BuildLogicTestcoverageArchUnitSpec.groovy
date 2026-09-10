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
import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import org.gradle.api.Plugin
import org.gradle.api.Task
import org.objectweb.asm.ClassReader
import org.objectweb.asm.ClassVisitor
import org.objectweb.asm.Handle
import org.objectweb.asm.MethodVisitor
import org.objectweb.asm.Opcodes
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes

/**
 * Architecture rules that enforce a minimum level of test coverage for the build logic in
 * {@code build-tools-internal}.
 *
 * <p>Rather than measuring line coverage, these rules assert that every production
 * <em>plugin</em> and <em>task</em> is paired with dedicated test coverage. This guards against
 * new build logic landing without any test at all.
 *
 * <ul>
 *   <li><b>Plugins</b> ({@link Plugin} implementations) must have either a {@code *FuncTest}
 *       that extends {@link AbstractGradleInternalPluginFuncTest}, exercising them against a real
 *       Gradle build via TestKit, or a ProjectBuilder-backed unit test that extends
 *       {@link AbstractProjectBuilderPluginSpec}.</li>
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
 * <p><b>Class discovery.</b> Production classes are imported from the compiled {@code main}
 * output. Plugin test harnesses are discovered from the compiled {@code test} and
 * {@code integTest} outputs that the dedicated {@code archunit} source set has on its classpath.
 * Task coverage still uses a lightweight filesystem scan of the {@code test} and {@code integTest}
 * source trees because those rules only need simple class-name matching.
 */
class BuildLogicTestCoverageArchUnitSpec extends AbstractArchUnitSpec {

    /** Test class name suffixes accepted as coverage for a task. */
    private static final List<String> TEST_SUFFIXES = ["Tests", "Test", "Spec", "FuncTest", "IT"]

    /**
     * Plugins and tasks that predate these coverage rules and are not yet tested. New entries must
     * not be added here — add a test instead. Existing entries should be removed as coverage is
     * filled in (the staleness test enforces this).
     */
    /**
     * Subclasses of {@link AbstractGradleInternalPluginFuncTest} that legitimately call
     * {@code disableConfigurationCache()} because the plugin under test has a known
     * configuration-cache incompatibility. New entries must not be added here without
     * first filing a follow-up to fix the underlying incompatibility.
     */
    private static final Set<String> KNOWN_CC_INCOMPATIBLE = [
        "BuildPluginFuncTest",
        "DraResolvePluginFuncTest",
        "ElasticsearchDistributionPluginFuncTest",
        "GlobalBuildInfoPluginFuncTest",
        "InternalBwcGitPluginFuncTest",
        "InternalDistributionBwcSetupPluginFuncTest",
        "JdkDownloadPluginFuncTest",
        "SnykDependencyMonitoringGradlePluginFuncTest",
    ] as Set

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
        "org.elasticsearch.gradle.internal.esql.EsqlFunctionPlugin",
        "org.elasticsearch.gradle.internal.packer.CacheTestFixtureResourcesPlugin",
        "org.elasticsearch.gradle.internal.precommit.DependencyLicensesPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.FilePermissionsPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ForbiddenApisPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.JarHellPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.LoggerUsagePrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ValidateRestSpecPlugin",
        "org.elasticsearch.gradle.internal.release.ReleaseToolsPlugin",
        "org.elasticsearch.gradle.internal.test.ClusterFeaturesMetadataPlugin",
        "org.elasticsearch.gradle.internal.test.DistroTestPlugin",
        "org.elasticsearch.gradle.internal.test.InternalClusterTestPlugin",
        "org.elasticsearch.gradle.internal.test.LegacyRestTestBasePlugin",
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
        "org.elasticsearch.gradle.internal.test.rest.CopyRestApiTask",
    ] as Set

    /** Production classes imported from the compiled {@code main} output only. */
    @Shared
    JavaClasses productionClasses = importProductionClasses()

    /** Compiled {@code test} and {@code integTest} classes visible to the {@code archunit} task. */
    @Shared
    JavaClasses pluginTestClasses = importPluginTestClasses()

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
    Set<String> pluginFuncTestNames = pluginTestClasses
        .findAll { JavaClass c ->
            c.isAssignableTo(AbstractGradleInternalPluginFuncTest)
                && c.modifiers.contains(JavaModifier.ABSTRACT) == false
                && topLevelName(c) == c.fullName
        }
        .collect { it.simpleName } as Set

    /**
     * Plugins covered by a ProjectBuilder-backed unit test that extends
     * {@link AbstractProjectBuilderPluginSpec}.
     */
    @Shared
    Map<String, Set<String>> projectBuilderPluginTestsByPlugin = discoverProjectBuilderPluginTestsByPlugin()

    def "every Gradle plugin is covered by either a FuncTest or a ProjectBuilder unit test"() {
        given:
        ArchRule rule = classes()
            .that().areAssignableTo(Plugin)
            .and().areTopLevelClasses()
            .and().areNotInterfaces()
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .and().resideOutsideOfPackage("org.elasticsearch.gradle.fixtures..")
            .should(beCoveredByAPluginTest())
            .because("every Gradle plugin must be covered either by a *FuncTest that extends "
                + AbstractGradleInternalPluginFuncTest.name
                + " or by a ProjectBuilder-backed unit test that extends "
                + AbstractProjectBuilderPluginSpec.name)

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

    def "no new AbstractGradleInternalPluginFuncTest subclass disables configuration cache"() {
        given:
        List<String> violations = pluginTestClasses
            .findAll { JavaClass c ->
                c.isAssignableTo(AbstractGradleInternalPluginFuncTest)
                && c.modifiers.contains(JavaModifier.ABSTRACT) == false
                && KNOWN_CC_INCOMPATIBLE.contains(c.simpleName) == false
                && callsDisableConfigurationCache(c)
            }
            .collect { it.fullName }
            .sort()

        expect:
        assert violations.isEmpty(), "These AbstractGradleInternalPluginFuncTest subclasses call disableConfigurationCache() -- fix the underlying plugin incompatibility instead:\n  ${violations.join('\n  ')}"
    }

    def "the cc-incompatible baseline contains no stale entries"() {
        given:
        Map<String, JavaClass> bySimpleName = pluginTestClasses.collectEntries { [(it.simpleName): it] }

        when:
        List<String> stale = KNOWN_CC_INCOMPATIBLE.findAll { String name ->
            JavaClass c = bySimpleName[name]
            c == null || callsDisableConfigurationCache(c) == false
        }.sort()

        then:
        assert stale.isEmpty(),
            "Stale KNOWN_CC_INCOMPATIBLE entries (fixed or removed) — delete them:\n  " + stale.join("\n  ")
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
     * Whether a subject is covered: plugins need either a corresponding func test extending
     * {@link AbstractGradleInternalPluginFuncTest} or a ProjectBuilder-backed unit test extending
     * {@link AbstractProjectBuilderPluginSpec}; every other subject (tasks) needs any test class
     * matching {@link #TEST_SUFFIXES}.
     */
    private boolean isCovered(JavaClass clazz) {
        if (clazz.isAssignableTo(Plugin)) {
            return pluginFuncTestNames.contains(clazz.simpleName + "FuncTest")
                || projectBuilderPluginTestsByPlugin.containsKey(clazz.fullName)
        }
        return TEST_SUFFIXES.any { testClassNames.contains(clazz.simpleName + it) }
    }

    /**
     * Builds a condition satisfied when a plugin is covered either by the TestKit-based internal
     * plugin harness or by a ProjectBuilder-backed unit spec. Classes listed in
     * {@link #KNOWN_UNCOVERED} are treated as an accepted baseline gap and never reported.
     */
    private ArchCondition<JavaClass> beCoveredByAPluginTest() {
        String funcBase = AbstractGradleInternalPluginFuncTest.simpleName
        String unitBase = AbstractProjectBuilderPluginSpec.simpleName
        return new ArchCondition<JavaClass>("be covered by a *FuncTest extending ${funcBase} or a ${unitBase}") {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                if (KNOWN_UNCOVERED.contains(item.fullName)) {
                    return // accepted baseline gap, see KNOWN_UNCOVERED
                }
                String expectedFuncTest = item.simpleName + "FuncTest"
                Set<String> projectBuilderTests = projectBuilderPluginTestsByPlugin[item.fullName] ?: [] as Set<String>
                if (pluginFuncTestNames.contains(expectedFuncTest) == false && projectBuilderTests.isEmpty()) {
                    events.add(SimpleConditionEvent.violated(
                        item,
                        "${item.fullName} has no ${expectedFuncTest} extending ${funcBase} and no ProjectBuilder unit test extending ${unitBase}"
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
     * Imports compiled classes from the {@code test} and {@code integTest} source sets only.
     * The dedicated {@code archunit} task adds both outputs to its classpath so these marker base
     * classes and concrete plugin tests can be inspected directly.
     */
    private static JavaClasses importPluginTestClasses() {
        return new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
            .withImportOption({ location ->
                location.contains("/classes/java/test/")
                    || location.contains("/classes/groovy/test/")
                    || location.contains("/classes/java/integTest/")
                    || location.contains("/classes/groovy/integTest/")
            } as ImportOption)
            .importPackages("org.elasticsearch.gradle")
    }

    private Map<String, Set<String>> discoverProjectBuilderPluginTestsByPlugin() {
        Map<String, Set<String>> byPlugin = [:].withDefault { new LinkedHashSet<String>() }
        pluginTestClasses.findAll { JavaClass c ->
            c.isAssignableTo(AbstractProjectBuilderPluginSpec)
                && c.modifiers.contains(JavaModifier.ABSTRACT) == false
                && topLevelName(c) == c.fullName
        }.each { JavaClass c ->
            try {
                Class<? extends AbstractProjectBuilderPluginSpec> specClass = (Class<? extends AbstractProjectBuilderPluginSpec>) Thread
                    .currentThread()
                    .contextClassLoader
                    .loadClass(c.fullName)
                AbstractProjectBuilderPluginSpec spec = specClass.getDeclaredConstructor().newInstance()
                byPlugin[spec.pluginClassUnderTest.name].add(c.simpleName)
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("Failed to inspect ProjectBuilder plugin test [${c.fullName}]", e)
            }
        }
        return byPlugin.collectEntries { String pluginName, Set<String> tests -> [(pluginName): tests.asImmutable()] }
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

    /**
     * Returns {@code true} if the compiled class for {@code javaClass} contains an
     * {@code invokedynamic} call to {@code disableConfigurationCache}.
     *
     * <p>Groovy compiles {@code disableConfigurationCache(...)} to an {@code invokedynamic}
     * instruction whose Groovy {@code IndyInterface} bootstrap receives the actual method
     * name as its first static argument. ASM's {@code visitInvokeDynamicInsn} exposes that
     * argument directly, making the detection bytecode-precise and comment-proof.
     */
    private static boolean callsDisableConfigurationCache(JavaClass javaClass) {
        String classPath = javaClass.fullName.replace('.', '/') + '.class'
        URL resource = Thread.currentThread().contextClassLoader.getResource(classPath)
        if (resource == null) return false
        boolean[] found = [false]
        resource.openStream().withCloseable { InputStream stream ->
            new ClassReader(stream).accept(new ClassVisitor(Opcodes.ASM9) {
                @Override
                MethodVisitor visitMethod(int access, String name, String desc, String sig, String[] exceptions) {
                    return new MethodVisitor(Opcodes.ASM9) {
                        @Override
                        void visitInvokeDynamicInsn(String name2, String desc2, Handle bsm, Object... bsmArgs) {
                            if (bsm.owner == 'org/codehaus/groovy/vmplugin/v8/IndyInterface'
                                    && bsm.name == 'bootstrap'
                                    && bsmArgs.length > 0
                                    && bsmArgs[0] == 'disableConfigurationCache') {
                                found[0] = true
                            }
                        }
                    }
                }
            }, 0)
        }
        return found[0]
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
