/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.nio.file.Path;
import java.util.List;

/**
 * How one test source set of one project can be re-run: the {@code Test} task paths that actually execute the
 * classes compiled into {@code outputDir}, as chosen by {@link TestTaskSelector}.
 *
 * <p>This exists so the scan step can run a class it did <em>not</em> resolve a ref to. Expanding an abstract
 * base is a repo-wide bytecode question, so it routinely turns up concrete subclasses compiled into a
 * different source-set output - another project's, or another source set of the same project. Such a subclass
 * cannot borrow the base target's {@code runnableTasks}, because those were selected against the base's own
 * output directory. Keying dispositions by {@code outputDir} lets the scan look up the right tasks for
 * whichever directory the subclass's bytecode actually came from.
 *
 * <p>Every project reports one of these per candidate test source set, whether or not it resolved a ref. That
 * is the point: the projects that own the interesting subclasses are usually not the ones the refs pointed at.
 *
 * @param sourceSet      the Gradle source-set name ({@code test}/{@code internalClusterTest}/...)
 * @param outputDir      the compiled-classes directory this disposition applies to (the lookup key)
 * @param kind           the wire kind for classes in this source set (see {@link Kinds})
 * @param runnableTasks  task paths that really run this source set's classes, capped and newest-first; empty
 *                       when {@code skipReason} is set
 * @param candidateTasks how many enabled candidates existed before the cap
 * @param skipReason     {@code null} when runnable, else why not (see {@link TestTaskSelector})
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SourceSetDisposition(
    String sourceSet,
    Path outputDir,
    String kind,
    List<String> runnableTasks,
    int candidateTasks,
    String skipReason
) {

    /** Whether this source set has at least one task that can re-run it on this agent. */
    public boolean runnable() {
        return skipReason == null;
    }
}
