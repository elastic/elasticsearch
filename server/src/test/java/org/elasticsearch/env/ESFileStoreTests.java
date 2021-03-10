/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.env;

import org.elasticsearch.test.ESTestCase;

import java.nio.file.FileStore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ESFileStoreTests extends ESTestCase {
    public void testNegativeSpace() throws Exception {
        FileStore mocked = mock(FileStore.class);
        when(mocked.getUsableSpace()).thenReturn(-1L);
        when(mocked.getTotalSpace()).thenReturn(-1L);
        when(mocked.getUnallocatedSpace()).thenReturn(-1L);
        assertEquals(-1, mocked.getUsableSpace());
        FileStore store = new ESFileStore(mocked);
        assertEquals(Long.MAX_VALUE, store.getUsableSpace());
        assertEquals(Long.MAX_VALUE, store.getTotalSpace());
        assertEquals(Long.MAX_VALUE, store.getUnallocatedSpace());
    }
}
