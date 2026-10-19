/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.stateless;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Models a cold store serving the files {@code counted} accepts in units of {@code unitSize} bytes, counting the units
 * fetched and the rounds waited one after another:
 * <ul>
 *   <li>reading a unit nobody prefetched is a round of its own;</li>
 *   <li>prefetching a unit adds it to the round in flight;</li>
 *   <li>reading a unit of the round in flight waits for, and completes, that round.</li>
 * </ul>
 * A unit is classified by its first read: presence, offsets or values.
 */
final class ReadRoundsDirectory extends FilterDirectory {

    /** What a unit holds. */
    enum Kind {
        PRESENCE,
        OFFSETS,
        VALUES
    }

    private final Predicate<String> counted;
    private final Function<String, Kind> kindOfFile;
    private final long unitSize;
    private final Map<String, Kind> kinds = new HashMap<>();
    private final EnumMap<Kind, Long> roundsByKind = new EnumMap<>(Kind.class);
    private final Set<String> fetched = new HashSet<>();
    private final Set<String> inFlight = new HashSet<>();
    private final Set<String> prefetched = new HashSet<>();
    private final Set<String> read = new HashSet<>();
    private boolean counting;
    private long rounds;
    private long unprefetchedReads;

    /** @param kindOfFile what a read holds when the reading code is neither {@code IndexedDISI} nor a packed reader */
    ReadRoundsDirectory(Directory in, Predicate<String> counted, Function<String, Kind> kindOfFile, long unitSize) {
        super(in);
        this.counted = counted;
        this.kindOfFile = kindOfFile;
        this.unitSize = unitSize;
    }

    /** Forgets everything fetched and starts counting. */
    void start() {
        fetched.clear();
        inFlight.clear();
        prefetched.clear();
        read.clear();
        rounds = 0;
        unprefetchedReads = 0;
        roundsByKind.clear();
        counting = true;
    }

    /** Rounds of fetches waited for one after another. */
    long rounds() {
        return rounds;
    }

    /** Units read that were not prefetched. */
    long unprefetchedReads() {
        return unprefetchedReads;
    }

    /** Units fetched. */
    long units() {
        return fetched.size() + inFlight.size();
    }

    /** Rounds waited for by reads of {@code kind}. */
    long rounds(Kind kind) {
        return roundsByKind.getOrDefault(kind, 0L);
    }

    /** Units of {@code kind} fetched. */
    long units(Kind kind) {
        return Stream.concat(fetched.stream(), inFlight.stream()).filter(unit -> kinds.get(unit) == kind).count();
    }

    /** Units prefetched and not read. */
    long unreadPrefetched() {
        return prefetched.stream().filter(unit -> read.contains(unit) == false).count();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        IndexInput in = super.openInput(name, context);
        return counted.test(name) ? new Counting(name, in, 0) : in;
    }

    private void onRead(String file, long from, long length) {
        if (counting == false || length <= 0) {
            return;
        }
        Kind kind = null;
        for (long unit = from / unitSize; unit <= (from + length - 1) / unitSize; unit++) {
            String key = file + "#" + unit;
            if (kinds.containsKey(key) == false) {
                kind = kind != null ? kind : kindOfRead(file);
                kinds.put(key, kind);
            }
            read.add(key);
            if (fetched.contains(key)) {
                continue;
            }
            rounds++;
            roundsByKind.merge(kinds.get(key), 1L, Long::sum);
            if (inFlight.contains(key)) {
                // Waits for the round in flight, which lands whole.
                fetched.addAll(inFlight);
                inFlight.clear();
            } else {
                fetched.add(key);
                unprefetchedReads++;
            }
        }
    }

    private void onPrefetch(String file, long from, long length) {
        if (counting == false || length <= 0) {
            return;
        }
        for (long unit = from / unitSize; unit <= (from + length - 1) / unitSize; unit++) {
            String key = file + "#" + unit;
            if (fetched.contains(key) == false && inFlight.add(key)) {
                prefetched.add(key);
            }
        }
    }

    /** Presence for {@code IndexedDISI}, offsets for packed readers, otherwise what {@code file} holds. */
    private Kind kindOfRead(String file) {
        Kind kind = StackWalker.getInstance().walk(frames -> frames.map(StackWalker.StackFrame::getClassName).map(name -> {
            if (name.startsWith("org.apache.lucene.codecs.lucene90.IndexedDISI")) {
                return Kind.PRESENCE;
            }
            if (name.startsWith("org.apache.lucene.util.packed.DirectReader")
                || name.startsWith("org.apache.lucene.util.packed.DirectMonotonicReader")) {
                return Kind.OFFSETS;
            }
            return null;
        }).filter(Objects::nonNull).findFirst().orElse(null));
        return kind != null ? kind : kindOfFile.apply(file);
    }

    private final class Counting extends IndexInput {
        private final String file;
        private final IndexInput in;
        private final long base;

        Counting(String file, IndexInput in, long base) {
            super("rounds(" + in + ")");
            this.file = file;
            this.in = in;
            this.base = base;
        }

        @Override
        public byte readByte() throws IOException {
            onRead(file, base + in.getFilePointer(), 1);
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            onRead(file, base + in.getFilePointer(), len);
            in.readBytes(b, offset, len);
        }

        @Override
        public void prefetch(long offset, long length) throws IOException {
            onPrefetch(file, base + offset, length);
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new Counting(file, in.slice(sliceDescription, offset, length), base + offset);
        }

        @Override
        public IndexInput clone() {
            return new Counting(file, in.clone(), base);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
