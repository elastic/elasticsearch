/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.fieldnames;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe field name canonicalization table using a parent/child pattern inspired by
 * Jackson's {@code ByteQuadsCanonicalizer}.
 *
 * <p>A single <strong>root</strong> instance is shared across all threads. Each parsing thread
 * obtains a <strong>child</strong> via {@link #makeChild()}, which starts with a read-only
 * snapshot of the parent's entries. Lookups hit the snapshot first (zero allocation on hit).
 * New names discovered during parsing are added to a thread-local overflow area. When parsing
 * is done, the child calls {@link Child#release()} to atomically merge new entries back into
 * the parent.
 *
 * <p>Hash table layout uses open addressing with linear probing. Short keys (&lt;= 16 bytes)
 * are compared via packed int quads; longer keys use {@code Arrays.equals} on stored
 * byte[] copies.
 */
public final class FieldNameTable {

    public static final int CAPACITY = 2048;
    public static final int MAX_COUNT = CAPACITY * 3 / 4;
    public static final int MAX_INLINE_BYTES = 16;
    public static final int MAX_INLINE_QUADS = MAX_INLINE_BYTES / Integer.BYTES;

    record Snapshot(String[] names, int[] hashes, int[] lens, int[] quads, byte[][] keys, int count) {
        Snapshot() {
            this(new String[CAPACITY], new int[CAPACITY], new int[CAPACITY], new int[CAPACITY * MAX_INLINE_QUADS], new byte[CAPACITY][], 0);
        }

        Snapshot copy() {
            return new Snapshot(
                Arrays.copyOf(names, names.length),
                Arrays.copyOf(hashes, hashes.length),
                Arrays.copyOf(lens, lens.length),
                Arrays.copyOf(quads, quads.length),
                Arrays.copyOf(keys, keys.length),
                count
            );
        }
    }

    private final AtomicReference<Snapshot> shared = new AtomicReference<>(new Snapshot());

    public FieldNameTable() {}

    public Child makeChild() {
        return new Child(this, shared.get());
    }

    void mergeChild(Snapshot childSnapshot, Snapshot parentSnapshot) {
        if (childSnapshot.count() <= parentSnapshot.count()) {
            return;
        }
        shared.compareAndSet(parentSnapshot, childSnapshot);
    }

    /**
     * Thread-confined child. Lookups check the parent snapshot first; new names are inserted
     * into a local copy (copy-on-write). Call {@link #release()} when done.
     */
    public static final class Child {

        private final FieldNameTable parent;
        private Snapshot parentSnapshot;

        public final String[] names;
        public final int[] hashes;
        public final int[] lens;
        public final int[] quads;
        public final byte[][] keys;

        public int count;
        public boolean dirty;

        Child(FieldNameTable parent, Snapshot snapshot) {
            this.parent = parent;
            this.parentSnapshot = snapshot;
            this.names = Arrays.copyOf(snapshot.names(), snapshot.names().length);
            this.hashes = Arrays.copyOf(snapshot.hashes(), snapshot.hashes().length);
            this.lens = Arrays.copyOf(snapshot.lens(), snapshot.lens().length);
            this.quads = Arrays.copyOf(snapshot.quads(), snapshot.quads().length);
            this.keys = Arrays.copyOf(snapshot.keys(), snapshot.keys().length);
            this.count = snapshot.count();
            this.dirty = false;
        }

        private void refreshFromSnapshot(Snapshot s) {
            if (s.count() > this.count) {
                System.arraycopy(s.names(), 0, names, 0, names.length);
                System.arraycopy(s.hashes(), 0, hashes, 0, hashes.length);
                System.arraycopy(s.lens(), 0, lens, 0, lens.length);
                System.arraycopy(s.quads(), 0, quads, 0, quads.length);
                System.arraycopy(s.keys(), 0, keys, 0, keys.length);
                this.count = s.count();
            }
            this.dirty = false;
        }

        public String lookupName(byte[] buf, int off, int len) {
            int h = FieldNameHash.hashName(buf, off, len);
            int slot = h & (CAPACITY - 1);
            for (int i = slot;; i = (i + 1) & (CAPACITY - 1)) {
                int sh = hashes[i];
                if (sh == 0) {
                    String s = new String(buf, off, len, StandardCharsets.UTF_8);
                    if (count < MAX_COUNT) {
                        dirty = true;
                        hashes[i] = h;
                        lens[i] = len;
                        names[i] = s;
                        if (len <= MAX_INLINE_BYTES) {
                            storeInlineQuads(i, buf, off, len);
                        } else {
                            keys[i] = Arrays.copyOfRange(buf, off, off + len);
                        }
                        count++;
                    }
                    return s;
                }
                if (sh == h && lens[i] == len && keysMatch(i, buf, off, len)) {
                    return names[i];
                }
            }
        }

        private boolean keysMatch(int i, byte[] buf, int off, int len) {
            if (len <= MAX_INLINE_BYTES) {
                int base = i * MAX_INLINE_QUADS;
                int fullQuads = len >>> 2;
                int tail = len & 3;
                for (int q = 0; q < fullQuads; q++) {
                    if (quads[base + q] != (int) FieldNameHash.intHandle().get(buf, off + q * Integer.BYTES)) {
                        return false;
                    }
                }
                int tailOff = off + fullQuads * Integer.BYTES;
                int storedTail = quads[base + fullQuads];
                return switch (tail) {
                    case 0 -> true;
                    case 1 -> (storedTail & 0xFF) == (buf[tailOff] & 0xFF);
                    case 2 -> (storedTail & 0xFFFF) == ((buf[tailOff] & 0xFF) | ((buf[tailOff + 1] & 0xFF) << 8));
                    case 3 -> storedTail == ((buf[tailOff] & 0xFF) | ((buf[tailOff + 1] & 0xFF) << 8) | ((buf[tailOff + 2] & 0xFF) << 16));
                    default -> throw new AssertionError();
                };
            }
            byte[] key = keys[i];
            return Arrays.equals(key, 0, key.length, buf, off, off + len);
        }

        private void storeInlineQuads(int i, byte[] buf, int off, int len) {
            int base = i * MAX_INLINE_QUADS;
            int fullQuads = len >>> 2;
            int tail = len & 3;
            for (int q = 0; q < fullQuads; q++) {
                quads[base + q] = (int) FieldNameHash.intHandle().get(buf, off + q * Integer.BYTES);
            }
            if (tail > 0) {
                int tailOff = off + fullQuads * Integer.BYTES;
                int t = buf[tailOff] & 0xFF;
                if (tail >= 2) t |= (buf[tailOff + 1] & 0xFF) << 8;
                if (tail == 3) t |= (buf[tailOff + 2] & 0xFF) << 16;
                quads[base + fullQuads] = t;
            }
        }

        public void release() {
            if (dirty) {
                Snapshot childSnap = new Snapshot(
                    Arrays.copyOf(names, names.length),
                    Arrays.copyOf(hashes, hashes.length),
                    Arrays.copyOf(lens, lens.length),
                    Arrays.copyOf(quads, quads.length),
                    Arrays.copyOf(keys, keys.length),
                    count
                );
                parent.mergeChild(childSnap, parentSnapshot);
                parentSnapshot = parent.shared.get();
                refreshFromSnapshot(parentSnapshot);
            } else {
                Snapshot latest = parent.shared.get();
                if (latest != parentSnapshot) {
                    parentSnapshot = latest;
                    refreshFromSnapshot(latest);
                }
            }
        }
    }
}
