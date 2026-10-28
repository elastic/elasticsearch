/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask;
import org.gradle.api.JavaVersion;
import org.gradle.api.Named;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.process.CommandLineArgumentProvider;

import java.io.File;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Configures a project to use the {@code java.lang.foreign} API without {@code --enable-preview}
 * on JDK 21. On JDK 22+ the Foreign Function and Memory API is standard, so this is effectively
 * a no-op.
 *
 * <p> Works by patching {@code java.base} at compile time with a stub JAR whose
 * {@code java.lang.foreign} classes have the {@code @PreviewFeature} annotation stripped. Also
 * enables forbidden-API checking for renamed preview APIs, so that direct usage of methods like
 * {@code getUtf8String} or {@code allocateUtf8String} is caught at build time.
 *
 * <p> Apply in a project's {@code build.gradle}:
 * <pre>{@code
 *   apply plugin: 'elasticsearch.foreign-api'
 * }</pre>
 */
public class ForeignApiPlugin implements Plugin<Project> {

    private static final String EXTRACT_FOREIGN_API_TASK_NAME = "extractForeignApiJar";

    private final JavaToolchainService javaToolchains;

    @Inject
    public ForeignApiPlugin(JavaToolchainService javaToolchains) {
        this.javaToolchains = javaToolchains;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);

        var buildParams = loadBuildParams(project).get();
        int minRuntime = Integer.parseInt(buildParams.getMinimumRuntimeVersion().getMajorVersion());

        if (minRuntime == 21) {
            // Register the extraction task and wire it into compilation only when the project
            // targets JDK 21. On JDK 22+ the Foreign Function & Memory API is standard, so neither
            // the stub JAR nor the --patch-module flag is needed and the task must not appear in
            // the task graph at all.
            TaskProvider<ExtractForeignApiTask> extractTask = project.getTasks()
                .register(EXTRACT_FOREIGN_API_TASK_NAME, ExtractForeignApiTask.class, t -> {
                    t.getOutputJar().set(project.getLayout().getBuildDirectory().file("jdk21-foreign-api.jar"));
                    // Only fork a worker when the Gradle daemon JVM is not already JDK 21. When
                    // the daemon runs on JDK 21, leaving the launcher unset lets the task run in
                    // the daemon ({@code noIsolation}), which avoids the cost of spawning a
                    // worker process. Mirrors the pattern in ForbiddenApisPrecommitPlugin.
                    if (JavaVersion.current().equals(JavaVersion.VERSION_21) == false) {
                        t.getJdk21Launcher()
                            .set(javaToolchains.launcherFor(spec -> spec.getLanguageVersion().set(JavaLanguageVersion.of(21))));
                    }
                });

            Provider<RegularFile> jarFile = extractTask.flatMap(ExtractForeignApiTask::getOutputJar);

            project.getTasks().withType(JavaCompile.class).configureEach(compileTask -> {
                var provider = new ForeignAccessArgumentProvider(jarFile, compileTask.getOptions().getRelease());
                compileTask.getOptions().getCompilerArgumentProviders().add(provider);
            });

            project.getTasks().withType(Javadoc.class).configureEach(javadocTask -> {
                javadocTask.dependsOn(extractTask);
                javadocTask.doFirst(t -> {
                    File jar = jarFile.get().getAsFile();
                    if (jar.exists()) {
                        CoreJavadocOptions options = (CoreJavadocOptions) javadocTask.getOptions();
                        options.addStringOption("-patch-module", "java.base=" + jar.getAbsolutePath());
                    }
                });
            });

            project.getTasks().withType(CheckForbiddenApisTask.class).configureEach(t -> t.checkForeignApiUsage(jarFile, 21));
        } else {
            // JDK 22+: foreign API is standard. No stub JAR is needed; pass null for the jar
            // parameter — checkForeignApiUsage only uses it when targetVersion == 21.
            project.getTasks().withType(CheckForbiddenApisTask.class).configureEach(t -> t.checkForeignApiUsage(null, minRuntime));
        }
    }

    /**
     * Provides {@code --patch-module java.base=<jar>} compiler arguments when the
     * compile release is 21 and the stub JAR exists. The release is always set on
     * every {@code JavaCompile} task by {@link ElasticsearchJavaBasePlugin}.
     */
    static class ForeignAccessArgumentProvider implements CommandLineArgumentProvider, Named {
        private final Provider<RegularFile> jarFile;
        private final Property<Integer> releaseProperty;

        ForeignAccessArgumentProvider(Provider<RegularFile> jarFile, Property<Integer> releaseProperty) {
            this.jarFile = jarFile;
            this.releaseProperty = releaseProperty;
        }

        @Override
        public Iterable<String> asArguments() {
            if (releaseProperty.isPresent() && releaseProperty.get() == 21 && jarFile.isPresent()) {
                File jar = jarFile.get().getAsFile();
                if (jar.exists()) {
                    return List.of("--patch-module", "java.base=" + jar.getAbsolutePath());
                }
            }
            return Collections.emptyList();
        }

        @InputFile
        @PathSensitive(PathSensitivity.RELATIVE)
        @Optional
        public Provider<RegularFile> getJarFile() {
            return jarFile;
        }

        @Internal
        @Override
        public String getName() {
            return "foreign-access-arg-provider";
        }
    }
}
