package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.repositories.IndexId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
            List<Future<String>> futures = new ArrayList<>();
            for (String candidate : deletionCandidates) {
                Future<String> future = executor.submit(() ->
                {
                    terminal.println(Terminal.Verbosity.VERBOSE, "Reading index " + candidate + " last modification timestamp");
                    Date indexTimestamp = getIndexTimestamp(candidate);
                    if (indexTimestamp != null) {
                        if (indexTimestamp.before(shiftedIndexNTimestamp)) {
                            terminal.println(Terminal.Verbosity.VERBOSE,
                                    "Index " + candidate + " is orphaned because it's modification timestamp " + indexTimestamp + " is " +
                                            "less than index-N shifted timestamp " + shiftedIndexNTimestamp);
                            return candidate;
                        } else {
                            terminal.println(Terminal.Verbosity.VERBOSE,
                                    "Index  " + candidate + " might not be orphaned because " + indexTimestamp +
                                            " is gte than " + shiftedIndexNTimestamp);
                        }
                    }
                    return null;
                });
                futures.add(future);
            }

            Set<String> orphanedIndexIds = new TreeSet<>();
            for (Future<String> future : futures) {
                try {
                    String indexId = future.get();
                    if (indexId != null) {
                        orphanedIndexIds.add(indexId);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            describeCollection("Set of orphaned indices", orphanedIndexIds);
            if (orphanedIndexIds.isEmpty()) {
                terminal.println(Terminal.Verbosity.NORMAL, "Set of orphaned indices is empty. Exiting");
                return;
            }
            confirm(terminal, "Do you want to remove orphaned indices files? This action is NOT REVERSIBLE");
            terminal.println(Terminal.Verbosity.NORMAL, "Removing orphaned indices");
            List<Future<Tuple<Integer, Long>>> removalFutures = new ArrayList<>();
            for (String indexId : orphanedIndexIds) {
                removalFutures.add(executor.submit(() -> {
                    terminal.println(Terminal.Verbosity.NORMAL, "Removing orphaned index " + indexId);
                    return deleteIndex(indexId);
                }));
            }

            int totalFilesRemoved = 0;
            long totalSpaceFreed = 0;
            for (Future<Tuple<Integer, Long>> future : removalFutures) {
                try {
                    Tuple<Integer, Long> removedFilesCountAndSize = future.get();
                    totalFilesRemoved += removedFilesCountAndSize.v1();
                    totalSpaceFreed += removedFilesCountAndSize.v2();
                } catch (Exception e) {
                    throw new ElasticsearchException(e);
                }
            }

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
