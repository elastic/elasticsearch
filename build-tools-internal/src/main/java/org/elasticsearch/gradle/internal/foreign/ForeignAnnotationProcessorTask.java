/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.foreign;

import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.compile.JavaCompile;

/**
 * Runs the {@code libs/foreign-library} annotation processor in {@code -proc:only} mode under a JDK 25
 * toolchain. The processor depends on the {@code java.lang.classfile} API (finalized in JDK 24), which
 * the Gradle daemon's bundled JDK 21 cannot load — hence the separate task.
 *
 * <p>The processor itself is a non-modular jar, discovered via {@code META-INF/services/
 * javax.annotation.processing.Processor}. Gradle's {@code JavaCompile} configures the in-process
 * compiler's file manager with {@code ANNOTATION_PROCESSOR_PATH}, which javac then uses in preference
 * to any {@code --processor-module-path} we could pass via compiler args; making the processor
 * non-modular sidesteps that limitation. See the discussion on PR #151164 for details.
 */
public abstract class ForeignAnnotationProcessorTask extends JavaCompile {

    /**
     * The minimum JDK version that can run the processor. The processor depends on the
     * {@code java.lang.classfile} API, finalized in JDK 24.
     */
    public static final int JDK_VERSION_FOR_PROCESSOR = 25;

    public ForeignAnnotationProcessorTask() {
        // We run under a JDK 25 toolchain and only need annotation processing — not bytecode generation.
        // Pinning to the consumer's release (e.g. 21) would reject sources that reference now-finalized
        // APIs that were preview in that release (e.g. MemorySegment in Java 21), so we compile against
        // the toolchain JDK instead. The processor's own -AjavaVersion option separately controls the
        // bytecode version of generated classes.
        setSourceCompatibility(Integer.toString(JDK_VERSION_FOR_PROCESSOR));
        setTargetCompatibility(Integer.toString(JDK_VERSION_FOR_PROCESSOR));
        getOptions().getCompilerArgs().add("-proc:only");
        getOptions().getCompilerArgumentProviders().add(() -> java.util.List.of("-AjavaVersion=" + getReleaseVersion().get()));
    }

    /** The {@code --release} version (e.g. {@code "21"}) passed to the processor via {@code -AjavaVersion}. */
    @Input
    public abstract Property<String> getReleaseVersion();
}
