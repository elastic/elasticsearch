/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;

public class ReleasableBytesStreamOutputTests extends ESTestCase {

    public void testRelease() throws Exception {
        MockPageCacheRecycler mockPageCacheRecycler = new MockPageCacheRecycler(Settings.EMPTY);
        try (ReleasableBytesStreamOutput output = getRandomReleasableBytesStreamOutput(mockPageCacheRecycler)) {
            output.writeBoolean(randomBoolean());
        }
        MockBigArrays.ensureAllArraysAreReleased();
    }

    private ReleasableBytesStreamOutput getRandomReleasableBytesStreamOutput(MockPageCacheRecycler pageCacheRecycler) throws IOException {
        ReleasableBytesStreamOutput output = new ReleasableBytesStreamOutput(new BytesRefRecycler(pageCacheRecycler));
        if (randomBoolean()) {
            for (int i = 0; i < scaledRandomIntBetween(1, 32); i++) {
                output.write(randomByte());
            }
        }
        return output;
    }
}
