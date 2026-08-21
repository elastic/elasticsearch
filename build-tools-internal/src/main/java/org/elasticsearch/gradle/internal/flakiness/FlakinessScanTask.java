/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.PlanEntry;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * The bytecode-enrichment (scan) half of the {@code resolve - compile - scan} Gradle flow. Runs as a
 * separate Gradle invocation <em>after</em> the compile step, so the compiled output directories named in the
 * resolved targets already exist on disk.
 *
 * <p>It reads the per-project outputs of {@link FlakinessResolveProjectTask} <b>directly</b> - there is no
 * merge task - folds them into one ordered target list ({@link FlakinessTargets#merge}), ASM-scans the
 * compiled classes of the bytecode-enriched kinds (flattening abstract bases into concrete subclasses), and
 * writes {@code flakiness-plan.json} (contract 2).
 *
 * <p>The task needs no project model - the per-project files already carry each project's authoritative
 * {@code classDirs} - so it is configuration-cache-clean: no {@code getProject()}, no live model, only managed
 * properties and plain file inputs.
 *
 * <h2>Why it is never up to date</h2>
 * The compiled bytecode it reads is an <b>undeclared</b> input: the directories are only known at execution
 * time, from the per-project files. Left as-is, Gradle would compare the declared inputs (the per-project
 * JSONs, the refs text, the caps) and report {@code UP-TO-DATE} across a recompile, serving a stale
 * {@code flakiness-plan.json} that names classes the current bytecode no longer contains.
 *
 * <p>Declaring the class directories instead would cost more than it saves: Gradle would content-hash every
 * class file in the repo (~7s for ~59k files) purely so ASM could immediately read them all again (~9s). So
 * the task opts out of state tracking rather than pretending its declared inputs are complete.
 *
 * <p>{@link org.gradle.api.Task#doNotTrackState} is used rather than
 * {@code getOutputs().upToDateWhen(t -> false)}: {@code DefaultTask.getOutputs()} is declared to return the
 * <em>internal</em> {@code TaskOutputsInternal}, which the build's own ArchUnit rule forbids production build
 * logic from touching. {@code doNotTrackState} is plain {@code Task} API, states the reason in Gradle's own
 * reporting, and additionally skips the pointless output snapshotting.
 */
public abstract class FlakinessScanTask extends DefaultTask {

    public FlakinessScanTask() {
        // See "Why it is never up to date" above: the bytecode this task reads is an undeclared input, so a
        // Gradle up-to-date verdict based on the declared ones would be wrong rather than merely stale.
        doNotTrackState("ASM-scans compiled bytecode whose directories are only known at execution time");
    }

    /**
     * The per-project {@code <project>.json} files written by {@code flakinessResolveProject}, collected from
     * {@link FlakinessProjectResolvePlugin#TARGETS_DIR}. A project that owns no ref writes an empty one, so an
     * absent file simply means that project's resolve task never ran.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getProjectTargetsFiles();

    /**
     * The whole {@code flakiness-refs.json} text. Needed only for the global {@code unresolved} verdict: a
     * class ref is unresolved exactly when no project claimed it, which no single project can decide.
     */
    @Input
    @Optional
    public abstract Property<String> getRefsJson();

    /** The refs file path, purely for a clear error message when the file is missing. */
    @Internal
    public abstract Property<String> getRefsPath();

    /** Deterministic cap on how many concrete subclasses of an abstract base to run. */
    @Input
    public abstract Property<Integer> getSubclassCap();

    /**
     * The {@code Test}-task fan-out cap the resolve step applied. Only used for the plan's
     * {@code taskSelections} report - the selection itself already happened in the resolve step, which is the
     * only step that has the project model.
     */
    @Input
    public abstract Property<Integer> getTaskCap();

    /**
     * The operator's {@code FLAKINESS_ITERS} / {@code -Pflakiness.iters} override for the iteration counts.
     * {@code @Optional}: absent leaves the defaults (100 unit / 20 internalClusterTest / 10 rest).
     */
    @Input
    @Optional
    public abstract Property<Integer> getIters();

    @OutputFile
    public abstract RegularFileProperty getPlanFile();

    @TaskAction
    public void scan() throws IOException {
        if (getRefsJson().isPresent() == false) {
            throw new GradleException(
                "flakiness-refs.json not found at "
                    + getRefsPath().getOrElse("flakiness-refs.json")
                    + "; the scan step expects the gather/bootstrap step to have written it. For a "
                    + "standalone run, pass -Pflakiness.refs=<path> pointing at a refs file."
            );
        }
        List<FlakinessRef> refs = FlakinessJson.parseRefs(getRefsJson().get()).refs();

        // Sorted so the fold is reproducible regardless of file-collection iteration order.
        List<File> files = getProjectTargetsFiles().getFiles().stream().sorted(Comparator.comparing(File::getPath)).toList();
        if (files.isEmpty() && refs.isEmpty() == false) {
            // Fail loudly rather than write a plan in which every ref is "unresolved". Zero per-project files
            // with refs to resolve means the resolve step did not run, ran without -Pflakiness.resolve, or had
            // its output directory removed - none of which are "these refs matched nothing". Writing a plan
            // here would produce a green build that silently re-runs no tests at all.
            throw new GradleException(
                "No per-project resolve output found under "
                    + FlakinessProjectResolvePlugin.TARGETS_DIR
                    + " but there are "
                    + refs.size()
                    + " refs to resolve. Run `flakinessResolveProject` (unqualified, with -Pflakiness.resolve) "
                    + "before flakinessScan; see JAVA_RESOLVER_NOTES.md."
            );
        }
        List<FlakinessJson.ProjectTargetsFile> perProject = new ArrayList<>(files.size());
        for (File f : files) {
            perProject.add(FlakinessJson.parseProjectTargets(Files.readString(f.toPath())));
        }
        FlakinessJson.BaseTargetsFile merged = FlakinessTargets.merge(refs, perProject);
        List<BaseTarget> targets = merged.targets();

        // Every project's compiled output, not just the owners' - see FlakinessTargets#classDirs for why an
        // abstract base and its subclasses are only connected when the scan set spans the whole repo.
        List<Path> classDirs = FlakinessTargets.classDirs(perProject);
        ClassHierarchyScanner scanner = ClassHierarchyScanner.scan(classDirs);
        getLogger().lifecycle("flakiness scan: ASM-scanned {} class directories across {} project files", classDirs.size(), files.size());

        // Lets PlanBuilder re-home a subclass found outside its base target's output onto the source set that
        // really owns it, instead of running it under tasks that do not contain it.
        Map<Path, FlakinessTargets.OwnedSourceSet> byDir = FlakinessTargets.dispositionsByClassDir(perProject);
        FlakinessPlan plan = PlanBuilder.build(
            targets,
            merged.unresolved(),
            scanner,
            getSubclassCap().get(),
            getTaskCap().get(),
            byDir::get
        );

        // Java owns batch-command generation now: attach the ready, target-neutral batch commands to the
        // plan so the TS generate step is a thin consumer (see CommandBuilder / PlanCommand).
        CommandBuilder.Config cfg = CommandBuilder.Config.defaults().withIterOverride(getIters().getOrNull());
        List<PlanEntry> runEntries = plan.entries().stream().filter(e -> Kinds.DISPOSITION_RUN.equals(e.disposition())).toList();
        plan = plan.withCommands(CommandBuilder.build(runEntries, cfg));

        File out = getPlanFile().get().getAsFile();
        out.getParentFile().mkdirs();
        Files.writeString(out.toPath(), FlakinessJson.writePlan(plan));
        getLogger().lifecycle(
            "flakiness plan: {} refs -> {} targets (from {} project files), {} entries, {} commands, {} expansions, "
                + "{} unresolved -> {}",
            refs.size(),
            targets.size(),
            files.size(),
            plan.entries().size(),
            plan.commands().size(),
            plan.expansions().size(),
            plan.unresolved().size(),
            out
        );
        for (BaseTarget t : targets) {
            if (t.runnable()) {
                getLogger().lifecycle("  {} {} {} -> {}", t.kind(), t.gradleProject(), identityOf(t), t.runnableTasks());
            } else {
                getLogger().lifecycle("  {} {} {} -> skip ({})", t.kind(), t.gradleProject(), identityOf(t), t.skipReason());
            }
        }
    }

    private static String identityOf(BaseTarget t) {
        return t.fqcn() != null ? t.fqcn() : t.suitePath() != null ? t.suitePath() : t.sourceSet();
    }
}
