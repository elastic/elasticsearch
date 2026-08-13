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
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The bytecode-enrichment (scan) half of the {@code resolve - compile - scan} Gradle flow. Runs as a
 * separate Gradle invocation <em>after</em> the compile step, so the compiled output directories named in
 * {@code flakiness-base-targets.json} already exist on disk. It reads those base targets, ASM-scans the
 * compiled classes of the bytecode-enriched kinds (flattening abstract bases into concrete subclasses), and
 * writes {@code flakiness-plan.json} (contract 2).
 *
 * <p>The task needs no cross-project model - the base targets already carry each source set's authoritative
 * {@code outputDir} - so it simply reads its input file at execution time. It is configuration-cache-clean:
 * no {@code getProject()}, no live model, only managed properties.
 */
public abstract class FlakinessScanTask extends DefaultTask {

    /** The whole {@code flakiness-base-targets.json} text, supplied lazily as an input. */
    @Input
    public abstract Property<String> getBaseTargetsJson();

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
        FlakinessJson.BaseTargetsFile input = FlakinessJson.parseBaseTargetsFile(getBaseTargetsJson().get());
        List<BaseTarget> targets = input.targets();

        ClassHierarchyScanner scanner = ClassHierarchyScanner.scan(scanDirs(targets));
        FlakinessPlan plan = PlanBuilder.build(targets, input.unresolved(), scanner, getSubclassCap().get(), getTaskCap().get());

        // Java owns batch-command generation now: attach the ready, target-neutral batch commands to the
        // plan so the TS generate step is a thin consumer (see CommandBuilder / PlanCommand).
        CommandBuilder.Config cfg = CommandBuilder.Config.defaults().withIterOverride(getIters().getOrNull());
        List<PlanEntry> runEntries = plan.entries().stream().filter(e -> Kinds.DISPOSITION_RUN.equals(e.disposition())).toList();
        plan = plan.withCommands(CommandBuilder.build(runEntries, cfg));

        File out = getPlanFile().get().getAsFile();
        out.getParentFile().mkdirs();
        Files.writeString(out.toPath(), FlakinessJson.writePlan(plan));
        getLogger().lifecycle(
            "flakiness plan: {} entries, {} commands, {} expansions, {} unresolved -> {}",
            plan.entries().size(),
            plan.commands().size(),
            plan.expansions().size(),
            plan.unresolved().size(),
            out
        );
    }

    /**
     * The distinct compiled-output directories to ASM-scan: only the bytecode-enriched, runnable targets. yaml
     * kinds carry no fqcn and targets with no runnable task are skipped, so neither contributes a scan dir.
     * Extracted as a pure static method so it is unit-testable without Gradle.
     */
    static List<Path> scanDirs(List<BaseTarget> targets) {
        Set<Path> classDirs = new LinkedHashSet<>();
        for (BaseTarget t : targets) {
            if (t.runnable() && Kinds.BYTECODE_ENRICHED.contains(t.kind()) && t.outputDir() != null) {
                classDirs.add(Path.of(t.outputDir()));
            }
        }
        return new ArrayList<>(classDirs);
    }
}
