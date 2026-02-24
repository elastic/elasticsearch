/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.MockBytesRefRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.hamcrest.Matchers.not;

public class SecureBytesRefRecyclerTests extends ESTestCase {
    public void testSecureBytesRefRecycler() {
        final BytesReference blankPage = new BytesArray(new byte[PageCacheRecycler.BYTE_PAGE_SIZE]);
        final var closedBlankPage = new AtomicBoolean();
        try (var mockRecycler = new MockBytesRefRecycler() {
            @Override
            protected void onClose(BytesRef bytesRef) {
                assertThat(new BytesArray(bytesRef), equalBytes(blankPage));
                assertTrue(closedBlankPage.compareAndSet(false, true));
            }
        }; var page = new SecureBytesRefRecycler(mockRecycler).obtain()) {
            assertThat(new BytesArray(page.v()), not(equalBytes(blankPage)));
        }
        assertTrue(closedBlankPage.get());
    }
}
