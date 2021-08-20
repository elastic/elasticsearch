/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.CRC32;

/**
 * The ID Generator creates a deterministic document ID to be used for rollup docs.
 * It does this by accepting values (e.g. composite keys for the rollup bucket) and
 * hashes those together in a deterministic manner.
 *
 * Once the ID has generated, the generator instance becomes locked and will not
 * accept any more values.  This is a safety mechanism to prevent accidentally
 * changing the ID at runtime.
 *
 * NOTE: this class is not thread safe; there is no synchronization on the "generated"
 * flag and it is unsafe to use from multiple threads.
 */
public abstract class RollupIDGenerator {
    public abstract void add(Integer v);

    public abstract void add(Long v);

    public abstract void add(Double v);

    public abstract void add(String v);

    public abstract void addNull();

    public abstract String getID();

    private boolean generated = false;

    final boolean check(Object v) {
        if (generated) {
            throw new RuntimeException("Cannot update ID as it has already been generated.");
        }
        if (v == null) {
            addNull();
            return false;
        }
        return true;
    }

    final void setFlag() {
        if (generated) {
            throw new RuntimeException("Cannot generate ID as it has already been generated.");
        }
        generated = true;
    }

    /**
     * The "old" style ID used in Rollup V1.  A rolling 32 bit CRC.
     *
     * Null values are hashed as (int)19.
     */
    @Deprecated
    public static class CRC extends RollupIDGenerator {
        private final CRC32 crc = new CRC32();

        @Override
        public void add(Integer v) {
            if (check(v)) {
                crc.update(v);
            }
        }

        @Override
        public void add(Long v) {
            if (check(v)) {
                crc.update(Numbers.longToBytes(v), 0, 8);
            }
        }

        @Override
        public void add(Double v) {
            if (check(v)) {
                crc.update(Numbers.doubleToBytes(v), 0, 8);
            }
        }

        @Override
        public void add(String v) {
            if (check(v)) {
                byte[] vs = (v).getBytes(StandardCharsets.UTF_8);
                crc.update(vs, 0, vs.length);
            }
        }

        @Override
        public void addNull() {
            // Old ID scheme used (int)19 as the null placeholder.
            // Not great but we're stuck with it :(
            crc.update(19);
        }

        @Override
        public String getID() {
            setFlag();
            return String.valueOf(crc.getValue());
        }
    }

    /**
     * The "new" style ID, used in Rollup V2.  A 128 bit Murmur3 hash of
     * all the keys concatenated together, base64-encoded, then prepended
     * with the job ID and a `$` delimiter
     *
     * Null values are hashed as a (hopefully) unique string `__NULL_PLACEHOLDER__830f1de2__`
     */
    public static class Murmur3 extends RollupIDGenerator {
        private static final long SEED = 19;
        private static final BytesRef DELIM = new BytesRef("$");
        private static final BytesRef NULL_PLACEHOLDER = new BytesRef("__NULL_PLACEHOLDER__830f1de2__");
        private final BytesRefBuilder id = new BytesRefBuilder();
        private final String jobId;

        Murmur3(String jobId) {
            this.jobId = jobId;
        }

        @Override
        public void add(Integer v) {
            if (check(v)) {
                update(Numbers.intToBytes(v));
            }
        }

        @Override
        public void add(Long v) {
            if (check(v)) {
                update(Numbers.longToBytes(v));
            }
        }

        @Override
        public void add(Double v) {
            if (check(v)) {
                update(Numbers.doubleToBytes(v));
            }
        }

        @Override
        public void add(String v) {
            if (check(v)) {
                update((v).getBytes(StandardCharsets.UTF_8));
            }
        }

        @Override
        public void addNull() {
            // New ID scheme uses a (hopefully) unique placeholder for null
            update(NULL_PLACEHOLDER.bytes);
        }

        private void update(byte[] v) {
            id.append(v, 0, v.length);
            id.append(DELIM);
        }

        @Override
        public String getID() {
            setFlag();
            MurmurHash3.Hash128 hasher = MurmurHash3.hash128(id.bytes(), 0, id.length(), SEED, new MurmurHash3.Hash128());
            byte[] hashedBytes = new byte[16];
            System.arraycopy(Numbers.longToBytes(hasher.h1), 0, hashedBytes, 0, 8);
            System.arraycopy(Numbers.longToBytes(hasher.h2), 0, hashedBytes, 8, 8);
            return jobId + "$" + Base64.getUrlEncoder().withoutPadding().encodeToString(hashedBytes);

        }
    }
}
