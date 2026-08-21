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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Gradle-free helpers over the per-project resolve outputs: folding them back into one ordered target list,
 * and unioning their compiled-output directories. Both are pure, so they are unit-testable without Gradle, and
 * both are consumed by {@link FlakinessScanTask}, which is the one step that needs the global view.
 *
 * <p>This is what used to be a separate root {@code flakinessMergeTargets} task. It needs no task of its own:
 * nothing between resolve and scan requires the merged view any more. The compile phase used to - it ran the
 * exact task list the resolve step derived - but it now invokes the {@code compile&lt;Ss&gt;Java} lifecycle
 * tasks unqualified, so it needs no input from resolve at all.
 */
public final class FlakinessTargets {

    private FlakinessTargets() {}

    /**
     * Fold every project's share of the answer into the single ordered target list the scan step consumes.
     * Two things genuinely require the global view and are therefore only done here:
     * <ul>
     *   <li><b>ref ordering</b> - each per-project file carries the index of the ref that produced each
     *       target, so the merged list reproduces the order of the refs file;</li>
     *   <li><b>the unresolved verdict</b> - a class ref is only unresolved if <em>no</em> project resolved it.
     *       Only {@code unmute}/{@code explicit} refs are surfaced as {@code no-source-file}; an unmatched
     *       {@code changed-file} ref is silently ignored (it is simply not a test). A ref carrying a
     *       {@code source} this resolver does not know is surfaced as {@code unknown-source}, mirroring
     *       {@link RefResolver#resolve}: the per-project verdicts are discarded, so without this the ref
     *       would vanish and a TS/Java contract drift would read as "nothing to run".</li>
     * </ul>
     *
     * @param perProject the parsed per-project files, in a deterministic order (the caller sorts by path), so
     *                   that two projects resolving the same ref break the tie reproducibly
     */
    public static FlakinessJson.BaseTargetsFile merge(List<FlakinessRef> refs, List<FlakinessJson.ProjectTargetsFile> perProject) {
        List<FlakinessJson.RefTarget> all = new ArrayList<>();
        BitSet resolvedRefs = new BitSet();
        for (FlakinessJson.ProjectTargetsFile file : perProject) {
            for (FlakinessJson.RefTarget rt : file.resolved()) {
                all.add(rt);
                resolvedRefs.set(rt.refIndex());
            }
        }

        // Stable sort by ref index restores the refs file's ordering; ties keep the caller's file order.
        all.sort(Comparator.comparingInt(FlakinessJson.RefTarget::refIndex));
        List<BaseTarget> targets = RefResolver.dedupe(all.stream().map(FlakinessJson.RefTarget::target).toList());

        List<FlakinessPlan.Unresolved> unresolved = new ArrayList<>();
        for (int i = 0; i < refs.size(); i++) {
            FlakinessRef ref = refs.get(i);
            if (resolvedRefs.get(i)) {
                continue;
            }
            boolean classRef = FlakinessRef.SOURCE_UNMUTE.equals(ref.source()) || FlakinessRef.SOURCE_EXPLICIT.equals(ref.source());
            if (classRef) {
                unresolved.add(new FlakinessPlan.Unresolved(ref, RefResolver.REASON_NO_SOURCE_FILE));
            } else if (FlakinessRef.SOURCE_CHANGED_FILE.equals(ref.source()) == false) {
                unresolved.add(new FlakinessPlan.Unresolved(ref, RefResolver.REASON_UNKNOWN_SOURCE));
            }
        }
        return new FlakinessJson.BaseTargetsFile(targets, unresolved);
    }

    /**
     * The union of every project's compiled-output directories, deduplicated and deterministically ordered -
     * i.e. the whole repo's bytecode, which is exactly what the ASM scan needs.
     *
     * <p>The union is taken over <em>all</em> per-project files rather than only the ones that resolved a ref.
     * That is the point: an abstract base in one project and its concrete subclasses in another are only
     * connected if both projects' output is in the scan set. Restricting the scan to the owning projects is
     * what used to make a cross-project hierarchy unresolvable.
     */
    public static List<Path> classDirs(List<FlakinessJson.ProjectTargetsFile> perProject) {
        TreeSet<Path> dirs = new TreeSet<>();
        for (FlakinessJson.ProjectTargetsFile file : perProject) {
            if (file.classDirs() != null) {
                dirs.addAll(file.classDirs());
            }
        }
        return new ArrayList<>(dirs);
    }

    /**
     * One project's {@link SourceSetDisposition} together with the project that reported it - what the scan
     * step needs to turn a class found in some directory into a runnable plan entry.
     */
    public record OwnedSourceSet(String projectPath, SourceSetDisposition disposition) {}

    /**
     * Index every project's source-set dispositions by compiled-output directory. This is the join that makes
     * cross-project abstract-base expansion actually <em>runnable</em>: the scanner reports which directory a
     * concrete subclass's bytecode came from, and this map turns that directory into the owning project, source
     * set, kind and real {@code Test} task paths - regardless of which project the originating ref named.
     *
     * <p>First writer wins if two projects somehow claim one directory, so the result is deterministic given
     * the caller's sorted file order.
     */
    public static Map<Path, OwnedSourceSet> dispositionsByClassDir(List<FlakinessJson.ProjectTargetsFile> perProject) {
        Map<Path, OwnedSourceSet> byDir = new HashMap<>();
        for (FlakinessJson.ProjectTargetsFile file : perProject) {
            if (file.dispositions() == null) {
                continue;
            }
            for (SourceSetDisposition d : file.dispositions()) {
                if (d.outputDir() != null) {
                    byDir.putIfAbsent(d.outputDir(), new OwnedSourceSet(file.projectPath(), d));
                }
            }
        }
        return byDir;
    }
}
