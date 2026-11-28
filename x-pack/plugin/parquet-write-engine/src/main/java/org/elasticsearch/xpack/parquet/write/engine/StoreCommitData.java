/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.engine;

import org.elasticsearch.index.store.Store;
import org.elasticsearch.xpack.parquet.write.flush.ShardCommitData;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The commit data of a shard in the one Lucene commit of its store.
 *
 * <p>The data is read from the store once and then kept in memory, and a commit replaces it. Reading it is not cheap: Lucene lists the
 * whole directory to find the latest commit, and the directory of this engine holds one file for each bulk that has not been uploaded.
 * With tens of thousands of such files, while bulks create and rename more of them, one read took about 20 seconds, and a flush reads it
 * for every run it writes (155 of the 365 busy seconds of the flush thread in a benchmark with 2,000-row bulks).
 *
 * <p>This is only correct while this engine is the one that changes the user data of the commit, and it is: the server writes it when it
 * creates or recovers a store, and then no engine is open. An instance belongs to one engine and is made when the engine is.
 */
final class StoreCommitData implements ShardCommitData {

    /** Reads the user data of the latest commit from the store. */
    @FunctionalInterface
    interface Reader {
        Map<String, String> read() throws IOException;
    }

    /** Writes the user data in a new commit. */
    @FunctionalInterface
    interface Writer {
        void write(Map<String, String> userData) throws IOException;
    }

    private final Reader reader;
    private final Writer writer;
    /** An unmodifiable copy of the user data that the last commit has, or null when it must be read from the store. */
    private volatile Map<String, String> current;
    /** One commit at a time, and a read that finds no data. A read of data that is known does not take it. */
    private final Object lock = new Object();
    /** Changes whenever the commit of the store may have changed, so a reader of something else in the commit knows to read it again. */
    private volatile long version;

    StoreCommitData(Store store) {
        this(() -> store.readLastCommittedSegmentsInfo().getUserData(), store::associateIndexWithNewUserData);
    }

    StoreCommitData(Reader reader, Writer writer) {
        this.reader = reader;
        this.writer = writer;
    }

    /** A number that is different after every commit through this object, and after a commit that failed. */
    long version() {
        return version;
    }

    @Override
    public Map<String, String> userData() throws IOException {
        Map<String, String> known = current;
        if (known == null) {
            synchronized (lock) {
                known = current;
                if (known == null) {
                    known = Map.copyOf(reader.read());
                    current = known;
                }
            }
        }
        return new HashMap<>(known);
    }

    @Override
    public void commit(Map<String, String> userData) throws IOException {
        final Map<String, String> copy = Map.copyOf(userData);
        synchronized (lock) {
            try {
                writer.write(copy);
            } catch (IOException | RuntimeException | Error e) {
                // the commit may or may not have happened: the next read asks the store
                current = null;
                version++;
                throw e;
            }
            current = copy;
            version++;
        }
    }
}
