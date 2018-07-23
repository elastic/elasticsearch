/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.shard;

import joptsimple.OptionSet;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Truncates the index using the previous returned status
 */
public class RemoveCorruptedSegmentsCommand extends ShardToolCommand {

    public RemoveCorruptedSegmentsCommand() {
        super("Remove corrupted segments from Lucene index", "Lucene index");
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("This tool removes the corrupted Lucene segments");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        warnAboutESShouldBeStopped(terminal);

        final PrintWriter writer = terminal.getWriter();
        final PrintStream printStream = new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
                writer.write(b);
            }
        }, false, "UTF-8");

        final ShardPath shardPath = getShardPath(options, env);

        final Path path = shardPath.resolveIndex();
        terminal.println("\nOpening index at " + path + "\n");
        Directory directory;
        try {
            directory = FSDirectory.open(path, NativeFSLockFactory.INSTANCE);
        } catch (Throwable t) {
            final String msg = "ERROR: could not open directory \"" + path + "\"; exiting";
            terminal.println(msg);
            throw new UserException(MISCONFIGURATION, msg);
        }

        try (Directory dir = directory;
             // Hold the lock open for the duration of the tool running
             Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            checkCorruptMarkerFileIsPresent(dir);

            final CheckIndex.Status status;
            try (CheckIndex checker = new CheckIndex(dir, writeLock)) {

                checker.setCrossCheckTermVectors(false);
                checker.setChecksumsOnly(true);
                checker.setInfoStream(printStream, terminal.isPrintable(Terminal.Verbosity.VERBOSE));

                status = checker.checkIndex(null);

                if (status.missingSegments == false) {
                    if (status.clean == false) {
                        warnMessage(terminal, status);

                        terminal.println("Writing...");
                        checker.exorciseIndex(status);

                        terminal.println("OK");
                        terminal.println("Wrote new segments file \"" + status.segmentsFileName + "\"");
                    } else {
                        terminal.println("Index is clean");
                    }
                } else {
                    throw new UserException(FAILURE, "Index is unrecoverable - there are missing segments");
                }
            }

            addNewHistoryCommit(directory, terminal, null);
            newAllocationId(env, shardPath, terminal);
            dropCorruptMarkerFiles(terminal, directory, status.clean);
        } catch (LockObtainFailedException lofe) {
            throw new UserException(MISCONFIGURATION,
                "Failed to lock shard's directory at [" + path + "], is Elasticsearch still running?");
        }
    }

    protected void checkCorruptMarkerFileIsPresent(Directory directory) throws IOException, UserException {
        if (isCorruptMarkerFileIsPresent(directory) == false) {
            throw new UserException(FAILURE, "There is no corruption file marker");
        }
    }

    /** Show a warning about losing data, asking for a confirmation */
    public static void warnMessage(Terminal terminal, CheckIndex.Status status) {
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        terminal.println(String.format(Locale.ROOT,
                  "! WARNING: Corrupted segments found - %8d documents will be lost.!",
                   status.totLoseDocCount));
        terminal.println("!                                                                     !");
        terminal.println("! WARNING:              YOU WILL LOSE DATA.                           !");
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        confirm("Continue and remove " + status.totLoseDocCount
            + " docs from the index ?", terminal);
    }

}
