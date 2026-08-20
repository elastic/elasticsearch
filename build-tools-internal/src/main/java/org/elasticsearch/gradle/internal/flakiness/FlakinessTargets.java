/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * Gradle-free helpers over the per-project resolve outputs: folding them back into one ordered target list,
 * and deriving the compile task paths. Both are pure, so they are unit-testable without Gradle and are shared
 * by the per-project resolve task (which derives its own compile task list) and {@link FlakinessScanTask}
 * (which folds every project's share together).
 *
 * <p>This is what used to be a separate root {@code flakinessMergeTargets} task. It needs no task of its own:
 * the only consumer that must run <em>before</em> compile is the compile task list, which each project can
 * derive for itself, and the only consumer that needs the global view is the scan step, which runs after.
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
     * The distinct compile task paths of the runnable targets, deterministically ordered. A target with no
     * runnable task is skipped downstream, so there is nothing to compile for it.
     */
    public static List<String> compileTaskPaths(List<BaseTarget> targets) {
        TreeSet<String> compileTasks = new TreeSet<>();
        for (BaseTarget t : targets) {
            if (t.runnable() && t.compileTaskPath() != null) {
                compileTasks.add(t.compileTaskPath());
            }
        }
        return new ArrayList<>(compileTasks);
    }
}
