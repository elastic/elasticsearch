/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.reindex.PaginatedHitSource.BasicHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

public class BasicHitTests extends ESTestCase {

    /**
     * Verifies that index, id, and version provided at construction are returned unchanged by their respective getters.
     * Verifies that optional fields are null or zero by default before any setters are invoked.
     */
    public void testConstructor() {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        long version = randomNonNegativeLong();
        PaginatedHitSource.BasicHit hit = new BasicHit(index, id, version);

        assertEquals(index, hit.getIndex());
        assertEquals(id, hit.getId());
        assertEquals(version, hit.getVersion());

        assertNull(hit.getSource());
        assertNull(hit.getXContentType());
        assertNull(hit.getRouting());
        assertEquals(0L, hit.getSeqNo());
        assertEquals(0L, hit.getPrimaryTerm());
    }

    /**
     * Verifies that setSource correctly sets both the source bytes and the associated XContentType.
     * Verifies that setSource returns the same instance, allowing fluent-style method chaining.
     */
    public void testSetSource() {
        BasicHit hit = new BasicHit(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), randomNonNegativeLong());
        BytesReference source = new BytesArray(randomAlphaOfLengthBetween(5, 50));
        XContentType xContentType = randomFrom(XContentType.values());

        BasicHit returned = hit.setSource(source, xContentType);
        assertSame(source, hit.getSource());
        assertEquals(xContentType, hit.getXContentType());
        assertSame(hit, returned);
    }

    /**
     * Verifies that routing can be set and retrieved correctly.
     * Verifies that setRouting returns the same instance, allowing fluent-style chaining.
     */
    public void testSetRouting() {
        BasicHit hit = new BasicHit(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), randomNonNegativeLong());
        String routing = randomAlphaOfLengthBetween(3, 20);
        BasicHit returned = hit.setRouting(routing);
        assertEquals(routing, hit.getRouting());
        assertSame(hit, returned);
    }

    /**
     * Verifies that sequence number can be set and retrieved correctly.
     */
    public void testSetSeqNo() {
        BasicHit hit = new BasicHit(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), randomNonNegativeLong());
        long seqNo = randomNonNegativeLong();
        hit.setSeqNo(seqNo);
        assertEquals(seqNo, hit.getSeqNo());
    }

    /**
     * Verifies that primary term can be set and retrieved correctly.
     */
    public void testSetPrimaryTerm() {
        BasicHit hit = new BasicHit(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), randomNonNegativeLong());
        long primaryTerm = randomNonNegativeLong();
        hit.setPrimaryTerm(primaryTerm);
        assertEquals(primaryTerm, hit.getPrimaryTerm());
    }

    /**
     * Verifies that setting all optional fields does not affect the required constructor-provided fields.
     */
    public void testOptionalSettersDoNotAffectRequiredFields() {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        long version = randomNonNegativeLong();
        BasicHit hit = new BasicHit(index, id, version);

        hit.setRouting(randomAlphaOfLengthBetween(3, 20));
        hit.setSeqNo(randomNonNegativeLong());
        hit.setPrimaryTerm(randomNonNegativeLong());
        hit.setSource(new BytesArray(randomAlphaOfLengthBetween(5, 50)), randomFrom(XContentType.values()));

        assertEquals(index, hit.getIndex());
        assertEquals(id, hit.getId());
        assertEquals(version, hit.getVersion());
    }
}
