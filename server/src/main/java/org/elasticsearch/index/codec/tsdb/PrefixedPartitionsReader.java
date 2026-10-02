/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Reads prefix-based partition metadata written by {@link PrefixedPartitionsWriter}.
 * The data consists of delta-encoded prefix keys and their corresponding starting document IDs.
 */
public final class PrefixedPartitionsReader {
    public static PartitionedDocValues.PrefixPartitions prefixPartitions(IndexInput dataIn, PartitionedDocValues.PrefixPartitions reused)
        throws IOException {
        final int numPartitions = dataIn.readVInt();
        final int[] prefixes;
        final int[] startDocs;
        if (reused == null || reused.numPartitions() < numPartitions) {
            prefixes = new int[numPartitions];
            startDocs = new int[numPartitions];
        } else {
            prefixes = reused.prefixes();
            startDocs = reused.startDocs();
        }
        int last = 0;
        for (int i = 0; i < numPartitions; i++) {
            final int diff = dataIn.readVInt();
            final int prefix = last + diff;
            prefixes[i] = prefix;
            last = prefix;
        }
        last = 0;
        for (int i = 0; i < numPartitions; i++) {
            final int diff = dataIn.readVInt();
            final int doc = last + diff;
            startDocs[i] = doc;
            last = doc;
        }
        return new PartitionedDocValues.PrefixPartitions(numPartitions, prefixes, startDocs);
    }
}
