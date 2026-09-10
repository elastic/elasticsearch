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
import org.gradle.api.Plugin
import org.objectweb.asm.ClassReader
import org.objectweb.asm.ClassVisitor
import org.objectweb.asm.FieldVisitor
import org.objectweb.asm.MethodVisitor
import org.objectweb.asm.Opcodes
import spock.lang.Shared

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes

/**
 * Guards against a build-logic plugin hard-coding Gradle project paths such as
 * {@code :benchmarks:common} or {@code :test:framework}.
 *
 * <p>Those literals couple a compiled binary plugin to the repository's current project layout,
 * making the plugin brittle to moves/renames and pushing repository-structure knowledge into code
 * that should instead take such relationships as explicit inputs. New plugins must not introduce
 * these literals; the existing usages are tracked in {@link #KNOWN_PROJECT_PATH_USAGES} until they
 * are migrated away.
 *
 * <p>The detection is bytecode-based rather than source-text-based: it scans each concrete plugin
 * class for String constants that look like Gradle project paths. That catches direct calls such as
 * {@code findProject(":foo")}, project dependencies declared with {@code Map.of("path", ":foo")},
 * and comparisons against {@code project.getPath()}, while ignoring comments and formatting.
 */
class ProjectPathUsageArchUnitSpec extends AbstractArchUnitSpec {

    /**
     * Plugin implementations that still embed hard-coded project path literals. New entries must
     * not be added — thread the relationship in via an extension, convention, or explicit input
     * instead. Existing entries should be removed as they are migrated.
     */
    private static final Set<String> KNOWN_PROJECT_PATH_USAGES = [
        "org.elasticsearch.gradle.internal.BaseInternalPluginBuildPlugin",
        "org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin",
        "org.elasticsearch.gradle.internal.InternalDistributionBwcSetupPlugin",
        "org.elasticsearch.gradle.internal.InternalDistributionDownloadPlugin",
        "org.elasticsearch.gradle.internal.esql.EsqlFunctionPlugin",
        "org.elasticsearch.gradle.internal.precommit.JarHellPrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.LoggerUsagePrecommitPlugin",
        "org.elasticsearch.gradle.internal.precommit.ThirdPartyAuditPrecommitPlugin",
        "org.elasticsearch.gradle.internal.test.ClusterFeaturesMetadataPlugin",
        "org.elasticsearch.gradle.internal.test.DistroTestPlugin",
        "org.elasticsearch.gradle.internal.test.StandaloneTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.InternalJavaRestTestPlugin",
        "org.elasticsearch.gradle.internal.test.rest.RestResourcesPlugin",
        "org.elasticsearch.gradle.internal.test.rest.RestTestBasePlugin",
    ] as Set

    private static final String PROJECT_PATH_PATTERN = /^:[A-Za-z0-9_.-]+(?::[A-Za-z0-9_.-]+)*$/

    @Shared
    JavaClasses productionClasses = importProductionClasses()

    def "plugin implementations do not embed hard-coded Gradle project paths"() {
        given:
        ArchRule rule = classes()
            .that().areAssignableTo(Plugin)
            .and().areTopLevelClasses()
            .and().areNotInterfaces()
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .and(notInBaseline(KNOWN_PROJECT_PATH_USAGES))
            .should(notEmbedHardCodedProjectPaths())
            .because("binary Gradle plugins should not hard-code repository project paths; pass the relationship as explicit wiring instead")

        expect:
        rule.check(productionClasses)
    }

    def "the project-path baseline contains no stale entries"() {
        expect:
        List<String> stale = staleBaselineEntries(KNOWN_PROJECT_PATH_USAGES, productionClasses) { JavaClass c ->
            c.isAssignableTo(Plugin) && hardCodedProjectPaths(c).isEmpty() == false
        }
        assert stale.isEmpty(), "Stale KNOWN_PROJECT_PATH_USAGES entries (migrated or removed) — delete them:\n  " + stale.join("\n  ")
    }

    private static ArchCondition<JavaClass> notEmbedHardCodedProjectPaths() {
        return new ArchCondition<JavaClass>("not embed hard-coded Gradle project paths") {
            @Override
            void check(JavaClass item, ConditionEvents events) {
                Set<String> paths = hardCodedProjectPaths(item)
                if (paths.isEmpty() == false) {
                    events.add(SimpleConditionEvent.violated(
                        item,
                        "${item.fullName} embeds hard-coded Gradle project paths ${paths}; pass these relationships as explicit inputs instead"
                    ))
                }
            }
        }
    }

    private static Set<String> hardCodedProjectPaths(JavaClass javaClass) {
        String classPath = javaClass.fullName.replace('.', '/') + '.class'
        URL resource = Thread.currentThread().contextClassLoader.getResource(classPath)
        if (resource == null) {
            return [] as Set<String>
        }

        Set<String> paths = new TreeSet<>()
        resource.openStream().withCloseable { InputStream stream ->
            new ClassReader(stream).accept(new ClassVisitor(Opcodes.ASM9) {
                @Override
                FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
                    if (value instanceof String && ((String) value) ==~ PROJECT_PATH_PATTERN) {
                        paths.add((String) value)
                    }
                    return null
                }

                @Override
                MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
                    return new MethodVisitor(Opcodes.ASM9) {
                        @Override
                        void visitLdcInsn(Object value) {
                            if (value instanceof String && ((String) value) ==~ PROJECT_PATH_PATTERN) {
                                paths.add((String) value)
                            }
                        }
                    }
                }
            }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES)
        }
        return paths
    }

}
