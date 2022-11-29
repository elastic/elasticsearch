/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.test.ESTestCase.randomUnicodeOfLengthBetween;

public class TranslogOperationsUtils {

    private TranslogOperationsUtils() {}

    /**
     * @return a new {@link Translog.Index} instance with the given id, sequence number, primary term and a random source.
     */
    public static Translog.Index indexOp(String id, long seqNo, long primaryTerm) {
        return indexOp(id, seqNo, primaryTerm, randomUnicodeOfLengthBetween(0, 20));
    }

    /**
     * @return a new {@link Translog.Index} instance with the given id, sequence number, primary term and source.
     */
    public static Translog.Index indexOp(String id, long seqNo, long primaryTerm, String source) {
        return new Translog.Index(
            id,
            seqNo,
            primaryTerm,
            Versions.MATCH_ANY,
            new BytesArray(Objects.requireNonNull(source).getBytes(StandardCharsets.UTF_8)),
            null,
            -1L
        );
    }
}
