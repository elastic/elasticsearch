/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.XContentType;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.randomFrom;

public class TranslogOperationsUtils {

    private TranslogOperationsUtils() {}

    /**
     * @return a random canonical {@link XContentType} suitable for a translog operation source.
     */
    public static XContentType randomSourceXContentType() {
        return randomFrom(XContentType.values()).canonical();
    }

    /**
     * @return a new {@link Translog.Index} instance with the given id, sequence number, primary term and a random source.
     */
    public static Translog.Index indexOp(String id, long seqNo, long primaryTerm) {
        final XContentType sourceContentType = randomSourceXContentType();
        return indexOp(id, seqNo, primaryTerm, RandomObjects.randomSource(random(), sourceContentType, 0), sourceContentType);
    }

    /**
     * @return a new {@link Translog.Index} instance with the given id, sequence number, primary term and source.
     */
    public static Translog.Index indexOp(String id, long seqNo, long primaryTerm, BytesReference source, XContentType sourceContentType) {
        return new Translog.Index(id, seqNo, primaryTerm, Versions.MATCH_ANY, source, sourceContentType, null, -1L);
    }
}
