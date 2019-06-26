package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.repositories.IndexId;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public abstract  class AbstractRepository implements Repository {
    protected final Terminal terminal;
    private final Long safetyGapMillis;

    protected AbstractRepository(Terminal terminal, Long safetyGapMillis) {
        this.terminal = terminal;
        this.safetyGapMillis = safetyGapMillis == null ? 3600 * 1000 : safetyGapMillis;
    }

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
        terminal.println(Terminal.Verbosity.VERBOSE, "Set of indices referenced by index file is " + referencedIndexIds);
        terminal.println(Terminal.Verbosity.VERBOSE, "Listing indices/ directory");
        Set<String> allIndexIds = getAllIndexIds();
        terminal.println(Terminal.Verbosity.VERBOSE, "Set of indices inside indices/ directory is " + allIndexIds);
        Set<String> deletionCandidates = Sets.difference(allIndexIds, referencedIndexIds);
        terminal.println(Terminal.Verbosity.VERBOSE, "Set of deletion candidates is " + deletionCandidates);
        if (deletionCandidates.isEmpty()) {
            terminal.println(Terminal.Verbosity.NORMAL, "Set of deletion candidates is empty. Exiting");
            return;
        }
        terminal.println(Terminal.Verbosity.VERBOSE, "Obtaining latest index file creation timestamp");

        Set<String> leakedIndexIds = new HashSet<>();
        for (String candidate : deletionCandidates) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Reading index " + candidate + " last modification timestamp");
            Date indexTimestamp = getIndexTimestamp(candidate);
            terminal.println(Terminal.Verbosity.VERBOSE, "Index " + candidate + " last modification timestamp is " + indexTimestamp);

            if (indexTimestamp.before(shiftedIndexNTimestamp)) {
                leakedIndexIds.add(candidate);
                terminal.println(Terminal.Verbosity.VERBOSE, "Index " + candidate + " has leaked because " + indexTimestamp + " is less " +
                        "than " + shiftedIndexNTimestamp);
            } else {
                terminal.println(Terminal.Verbosity.VERBOSE, "Index  " + candidate + " might not be leaked because " + indexTimestamp +
                        " is gte than " + shiftedIndexNTimestamp);
            }
        }
        terminal.println(Terminal.Verbosity.NORMAL, "Set of leaked indices is " + leakedIndexIds);
        confirm(terminal, "Do you want to remove leaked indices files? This action is NOT REVERSIBLE");
        terminal.println(Terminal.Verbosity.NORMAL, "Removing leaked indices");
        deleteIndices(leakedIndexIds);
        terminal.println(Terminal.Verbosity.NORMAL, "Finished removing leaked indices");
    }

    protected void confirm(Terminal terminal, String msg) {
        terminal.println(Terminal.Verbosity.NORMAL, msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException("Aborted by user");
        }
    }
}
