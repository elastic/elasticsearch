package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.repositories.IndexId;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public abstract class AbstractRepository implements Repository {
    private final static long DEFAULT_SAFETY_GAP_MILLIS = 3600 * 1000;
    private static final int DEFAULT_PARALLELISM = 100;

    protected final Terminal terminal;
    private final long safetyGapMillis;
    private final int parallelism;

    protected AbstractRepository(Terminal terminal, Long safetyGapMillis, Integer parallelism) {
        this.terminal = terminal;
        this.safetyGapMillis = safetyGapMillis == null ? DEFAULT_SAFETY_GAP_MILLIS : safetyGapMillis;
        this.parallelism = parallelism == null ? DEFAULT_PARALLELISM : parallelism;
    }

    private void describeCollection(String start, Collection<?> elements) {
        terminal.println(Terminal.Verbosity.VERBOSE,
                start  + " has " + elements.size() + " elements: " + elements);
    }

    @Override
    public void cleanup() throws IOException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Obtaining latest index file generation and creation timestamp");
        Tuple<Long, Date> latestIndexIdAndTimestamp = getLatestIndexIdAndTimestamp();
        if (latestIndexIdAndTimestamp.v1() == -1) {
            terminal.println(Terminal.Verbosity.NORMAL, "No index-N files found. Repository is empty or corrupted? Exiting");
            return;
        }
        long latestIndexId = latestIndexIdAndTimestamp.v1();
        terminal.println(Terminal.Verbosity.VERBOSE, "Latest index file generation is " + latestIndexId);
        Date indexNTimestamp = latestIndexIdAndTimestamp.v2();
        Date shiftedIndexNTimestamp = new Date(indexNTimestamp.getTime() - safetyGapMillis);
        terminal.println(Terminal.Verbosity.VERBOSE, "Latest index file creation timestamp is " + indexNTimestamp);
        terminal.println(Terminal.Verbosity.VERBOSE, "Shifted by safety gap creation timestamp is " + shiftedIndexNTimestamp);

        terminal.println(Terminal.Verbosity.VERBOSE, "Reading latest index file");
        Set<String> referencedIndexIds = getRepositoryData(latestIndexId)
                .getIndices().values().stream().map(IndexId::getId).collect(Collectors.toSet());
        describeCollection("Set of indices referenced by index file", referencedIndexIds);

        terminal.println(Terminal.Verbosity.VERBOSE, "Listing indices/ directory");
        Set<String> allIndexIds = getAllIndexDirectoryNames();
        describeCollection("Set of indices inside indices/ directory", allIndexIds);

        Set<String> deletionCandidates = new TreeSet<>(Sets.difference(allIndexIds, referencedIndexIds));
        describeCollection("Set of deletion candidates", deletionCandidates);
        if (deletionCandidates.isEmpty()) {
            terminal.println(Terminal.Verbosity.NORMAL, "Set of deletion candidates is empty. Exiting");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        try {
            PlainActionFuture<Collection<String>> orphanedIndicesFuture = new PlainActionFuture<>();
            GroupedActionListener<String> groupedOrphanedIndicesListener = new GroupedActionListener<>(orphanedIndicesFuture,
                    deletionCandidates.size());
            for (String candidate : deletionCandidates) {
                executor.submit(new ActionRunnable<>(groupedOrphanedIndicesListener) {
                    @Override
                    protected void doRun() {
                        terminal.println(Terminal.Verbosity.VERBOSE, "Reading index " + candidate + " last modification timestamp");
                        Date indexTimestamp = getIndexTimestamp(candidate);
                        if (indexTimestamp != null) {
                            if (indexTimestamp.before(shiftedIndexNTimestamp)) {
                                terminal.println(Terminal.Verbosity.VERBOSE,
                                        "Index " + candidate + " is orphaned because it's modification timestamp " + indexTimestamp + " is " +
                                                "less than index-N shifted timestamp " + shiftedIndexNTimestamp);
                                groupedOrphanedIndicesListener.onResponse(candidate);
                            } else {
                                terminal.println(Terminal.Verbosity.VERBOSE,
                                        "Index  " + candidate + " might not be orphaned because " + indexTimestamp +
                                                " is gte than " + shiftedIndexNTimestamp);
                                groupedOrphanedIndicesListener.onResponse(null);
                            }
                        } else {
                            groupedOrphanedIndicesListener.onResponse(null);
                        }
                    }
                });
            }
            Set<String> orphanedIndexIds =
                    new TreeSet<>(orphanedIndicesFuture.actionGet().stream().filter(Objects::nonNull).collect(Collectors.toSet()));
            describeCollection("Set of orphaned indices", orphanedIndexIds);
            if (orphanedIndexIds.isEmpty()) {
                terminal.println(Terminal.Verbosity.NORMAL, "Set of orphaned indices is empty. Exiting");
                return;
            }

            confirm(terminal, "Do you want to remove orphaned indices files? This action is NOT REVERSIBLE");

            terminal.println(Terminal.Verbosity.NORMAL, "Removing orphaned indices");
            PlainActionFuture<Collection<Tuple<Integer, Long>>> removalFuture = new PlainActionFuture<>();
            GroupedActionListener<Tuple<Integer, Long>> groupedRemovalListener =
                    new GroupedActionListener<>(removalFuture, orphanedIndexIds.size());
            for (final String indexId : orphanedIndexIds) {
                executor.submit(new ActionRunnable<>(groupedRemovalListener) {
                    @Override
                    protected void doRun() {
                        terminal.println(Terminal.Verbosity.NORMAL,"Removing orphaned index "+indexId);
                        groupedRemovalListener.onResponse(deleteIndex(indexId));
                    }
                });
            }
            Collection<Tuple<Integer, Long>> removeResults = removalFuture.actionGet();
            int totalFilesRemoved = removeResults.stream().map(Tuple::v1).reduce(0, (a, b) -> a + b);
            long totalSpaceFreed = removeResults.stream().map(Tuple::v2).reduce(0L, (a, b) -> a + b);
            terminal.println(Terminal.Verbosity.NORMAL, "In total removed " + totalFilesRemoved + " files");
            terminal.println(Terminal.Verbosity.NORMAL, "In total space freed " + totalSpaceFreed + " bytes");
            terminal.println(Terminal.Verbosity.NORMAL, "Finished removing orphaned indices");
        } finally {
            executor.shutdown();
        }
    }

    private void confirm(Terminal terminal, String msg) {
        terminal.println(Terminal.Verbosity.NORMAL, msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException("Aborted by user");
        }
    }
}
