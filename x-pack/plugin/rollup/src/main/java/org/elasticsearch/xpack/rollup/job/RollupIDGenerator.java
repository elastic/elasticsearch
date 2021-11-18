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
 *
 *  A 128 bit Murmur3 hash of all the keys concatenated together, base64-encoded, then
 *  prepended with the job ID and a `$` delimiter
 *
 *  Null values are hashed as a (hopefully) unique string `__NULL_PLACEHOLDER__830f1de2__`
 */
public class RollupIDGenerator {
    private boolean generated = false;
    private static final long SEED = 19;
    private static final BytesRef DELIM = new BytesRef("$");
    private static final BytesRef NULL_PLACEHOLDER = new BytesRef("__NULL_PLACEHOLDER__830f1de2__");
    private final BytesRefBuilder id = new BytesRefBuilder();
    private final String jobId;

    RollupIDGenerator(String jobId) {
        this.jobId = jobId;
    }

    public void add(Integer v) {
        if (check(v)) {
            update(Numbers.intToBytes(v));
        }
    }

    public void add(Long v) {
        if (check(v)) {
            update(Numbers.longToBytes(v));
        }
    }

    public void add(Double v) {
        if (check(v)) {
            update(Numbers.doubleToBytes(v));
        }
    }

    public void add(String v) {
        if (check(v)) {
            update((v).getBytes(StandardCharsets.UTF_8));
        }
    }

    void addNull() {
        // New ID scheme uses a (hopefully) unique placeholder for null
        update(NULL_PLACEHOLDER.bytes);
    }

    private void update(byte[] v) {
        id.append(v, 0, v.length);
        id.append(DELIM);
    }

    private boolean check(Object v) {
        if (generated) {
            throw new RuntimeException("Cannot update ID as it has already been generated.");
        }
        if (v == null) {
            addNull();
            return false;
        }
        return true;
    }

    private void setFlag() {
        if (generated) {
            throw new RuntimeException("Cannot generate ID as it has already been generated.");
        }
        generated = true;
    }

    public String getID() {
        setFlag();
        MurmurHash3.Hash128 hasher = MurmurHash3.hash128(id.bytes(), 0, id.length(), SEED, new MurmurHash3.Hash128());
        byte[] hashedBytes = new byte[16];
        System.arraycopy(Numbers.longToBytes(hasher.h1), 0, hashedBytes, 0, 8);
        System.arraycopy(Numbers.longToBytes(hasher.h2), 0, hashedBytes, 8, 8);
        return jobId + "$" + Base64.getUrlEncoder().withoutPadding().encodeToString(hashedBytes);

    }

}
