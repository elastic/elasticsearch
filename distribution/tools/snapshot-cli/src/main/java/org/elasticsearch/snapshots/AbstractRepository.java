package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.repositories.IndexId;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public abstract class AbstractRepository implements Repository {
    protected final Terminal terminal;
    private final long safetyGapMillis;

    protected AbstractRepository(Terminal terminal, Long safetyGapMillis) {
        this.terminal = terminal;
        this.safetyGapMillis = safetyGapMillis == null ? 3600 * 1000 : safetyGapMillis;
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
        terminal.println(Terminal.Verbosity.VERBOSE, "Obtaining latest index file creation timestamp");

        Set<String> orphanedIndexIds = new TreeSet<>();
        for (String candidate : deletionCandidates) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Reading index " + candidate + " last modification timestamp");
            Date indexTimestamp = getIndexTimestamp(candidate);
            if (indexTimestamp != null) {
                if (indexTimestamp.before(shiftedIndexNTimestamp)) {
                    orphanedIndexIds.add(candidate);
                    terminal.println(Terminal.Verbosity.VERBOSE,
                            "Index " + candidate + " is orphaned because it's modification timestamp " + indexTimestamp + " is " +
                            "less than index-N shifted timestamp " + shiftedIndexNTimestamp);
                } else {
                    terminal.println(Terminal.Verbosity.VERBOSE, "Index  " + candidate + " might not be orphaned because " + indexTimestamp +
                            " is gte than " + shiftedIndexNTimestamp);
                }
            }
        }
        describeCollection("Set of orphaned indices", orphanedIndexIds);
        if (orphanedIndexIds.isEmpty()) {
            terminal.println(Terminal.Verbosity.NORMAL, "Set of orphaned indices is empty. Exiting");
            return;
        }
        confirm(terminal, "Do you want to remove orphaned indices files? This action is NOT REVERSIBLE");
        terminal.println(Terminal.Verbosity.NORMAL, "Removing orphaned indices");
        deleteIndices(orphanedIndexIds);
        terminal.println(Terminal.Verbosity.NORMAL, "Finished removing orphaned indices");
    }

    private void confirm(Terminal terminal, String msg) {
        terminal.println(Terminal.Verbosity.NORMAL, msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException("Aborted by user");
        }
    }
}
