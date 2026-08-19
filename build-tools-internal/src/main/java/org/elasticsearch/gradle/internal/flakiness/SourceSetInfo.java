/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.nio.file.Path;
import java.util.List;

/**
 * A Gradle-free snapshot of one test source set of one project, captured from the project's <em>own</em>
 * configured model (never a cross-project read) and carried to the resolve task as a task input.
 *
 * <p>Every field is authoritative - read straight off the live {@code SourceSet} / compile task at the
 * project's own configuration time (see {@link FlakinessProjectModel}):
 * <ul>
 *   <li>{@code javaSrcDirs} / {@code resourceSrcDirs} - the real {@code srcDirs}, so path-&gt;source-set
 *       resolution and the class-ref filesystem probe no longer assume the {@code src/&lt;ss&gt;/java}
 *       convention;</li>
 *   <li>{@code outputDir} - the real compiled-classes directory the ASM scan reads;</li>
 *   <li>{@code compileTaskPath} - the real {@code compile&lt;Ss&gt;Java} task path, so the compile step
 *       compiles exactly what will run.</li>
 * </ul>
 *
 * @param name            the Gradle source-set name ({@code test}/{@code internalClusterTest}/
 *                        {@code javaRestTest}/{@code yamlRestTest})
 * @param javaSrcDirs     absolute java source roots of this source set
 * @param resourceSrcDirs absolute resource source roots of this source set (used to locate yaml suites)
 * @param outputDir       absolute compiled-classes output directory ({@code build/classes/java/&lt;ss&gt;})
 * @param compileTaskPath the fully-qualified Gradle task path that compiles this source set
 */
public record SourceSetInfo(String name, List<Path> javaSrcDirs, List<Path> resourceSrcDirs, Path outputDir, String compileTaskPath) {}
