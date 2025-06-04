/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.shard;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Removes corrupted Lucene index segments
 */
public class RemoveCorruptedLuceneSegmentsAction {

    public static Tuple<RemoveCorruptedShardDataCommand.CleanStatus, String> getCleanStatus(
        Directory indexDirectory,
        Lock writeLock,
        PrintStream printStream,
        boolean verbose
    ) throws IOException {
        boolean markedCorrupted = RemoveCorruptedShardDataCommand.isCorruptMarkerFileIsPresent(indexDirectory);

        final CheckIndex.Status status;
        try (CheckIndex checker = new CheckIndex(indexDirectory, writeLock)) {
            checker.setLevel(CheckIndex.Level.MIN_LEVEL_FOR_CHECKSUM_CHECKS);
            checker.setInfoStream(printStream, verbose);

            status = checker.checkIndex(null);

            if (status.missingSegments) {
                return Tuple.tuple(
                    RemoveCorruptedShardDataCommand.CleanStatus.UNRECOVERABLE,
                    "Index is unrecoverable - there are missing segments"
                );
            }

            return status.clean
                ? Tuple.tuple(
                    markedCorrupted
                        ? RemoveCorruptedShardDataCommand.CleanStatus.CLEAN_WITH_CORRUPTED_MARKER
                        : RemoveCorruptedShardDataCommand.CleanStatus.CLEAN,
                    null
                )
                : Tuple.tuple(
                    RemoveCorruptedShardDataCommand.CleanStatus.CORRUPTED,
                    "Corrupted Lucene index segments found - " + status.totLoseDocCount + " documents will be lost."
                );
        }
    }

    public static void execute(Terminal terminal, Directory indexDirectory, Lock writeLock, PrintStream printStream, boolean verbose)
        throws IOException {
        final CheckIndex.Status status;
        try (CheckIndex checker = new CheckIndex(indexDirectory, writeLock)) {

            checker.setLevel(CheckIndex.Level.MIN_LEVEL_FOR_CHECKSUM_CHECKS);
            checker.setInfoStream(printStream, verbose);

            status = checker.checkIndex(null);

            if (status.missingSegments == false) {
                if (status.clean == false) {
                    terminal.println("Writing...");
                    checker.exorciseIndex(status);

                    terminal.println("OK");
                    terminal.println("Wrote new segments file \"" + status.segmentsFileName + "\"");
                }
            } else {
                throw new ElasticsearchException("Index is unrecoverable - there are missing segments");
            }
        }
    }
}
