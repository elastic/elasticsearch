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
 *   <li>{@code outputDir} - the real compiled-classes directory. This is the authoritative
 *       <em>disposition</em> query: {@link TestTaskSelector} decides which {@code Test} tasks actually run
 *       this source set's classes by intersecting it with each task's {@code testClassesDirs}.</li>
 * </ul>
 *
 * <p>There is deliberately no {@code compileTaskPath}: the compile phase invokes the four
 * {@code compile&lt;Ss&gt;Java} lifecycle tasks <em>unqualified</em> so every project compiles, which means no
 * per-source-set task path needs deriving or carrying (see {@link FlakinessScanTask}).
 *
 * @param name            the Gradle source-set name ({@code test}/{@code internalClusterTest}/
 *                        {@code javaRestTest}/{@code yamlRestTest})
 * @param javaSrcDirs     absolute java source roots of this source set
 * @param resourceSrcDirs absolute resource source roots of this source set (used to locate yaml suites)
 * @param outputDir       absolute compiled-classes output directory ({@code build/classes/java/&lt;ss&gt;})
 */
public record SourceSetInfo(String name, List<Path> javaSrcDirs, List<Path> resourceSrcDirs, Path outputDir) {}
