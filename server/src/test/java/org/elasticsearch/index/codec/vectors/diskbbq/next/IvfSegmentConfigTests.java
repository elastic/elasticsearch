/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.is;

public class IvfSegmentConfigTests extends ESTestCase {

    public void testFromCodecDefaultsUsesNaNOversampling() {
        var q = ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC;
        IvfSegmentConfig c = IvfSegmentConfig.fromCodecDefaults(q, true);
        assertThat(c.quantEncoding(), is(q));
        assertTrue(c.usePrecondition());
        assertTrue(Float.isNaN(c.rescoreOversample()));
    }

    public void testEmptyFlushSource() throws Exception {
        IvfFlushConfigSource src = IvfFlushConfigSource.empty();
        assertEquals(Optional.empty(), src.load(null, null));
    }

    public void testMergeResolverReturnsCodecDefault() throws Exception {
        IvfMergeConfigResolver r = IvfMergeConfigResolver.useCodecDefault();
        IvfSegmentConfig def = IvfSegmentConfig.fromCodecDefaults(ESNextDiskBBQVectorsFormat.QuantEncoding.SEVEN_BIT_SYMMETRIC, false);
        assertSame(def, r.resolve(null, null, def));
    }
}
