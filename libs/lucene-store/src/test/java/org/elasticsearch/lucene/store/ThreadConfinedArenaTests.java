/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ThreadConfinedArenaTests extends ESTestCase {

    public void testNoArenaOutsideScope() {
        assertThat(ThreadConfinedArena.CURRENT.get(), nullValue());
        MemorySegment segment = ThreadConfinedArena.allocate(16, 8);
        assertThat(segment.scope().isAlive(), is(true));
    }

    public void testArenaClosedWhenScopeExits() {
        MemorySegment segment = ThreadConfinedArena.withConfinedArena(() -> {
            assertThat(ThreadConfinedArena.CURRENT.get(), notNullValue());
            return ThreadConfinedArena.allocate(16, 8);
        });
        assertThat(segment.scope().isAlive(), is(false));
        assertThat(ThreadConfinedArena.CURRENT.get(), nullValue());
    }

    public void testNestedScopeRestoresOuterArena() {
        AtomicReference<MemorySegment> inner = new AtomicReference<>();
        ThreadConfinedArena.withConfinedArena(() -> {
            Arena outer = ThreadConfinedArena.CURRENT.get();
            ThreadConfinedArena.withConfinedArena(() -> {
                assertThat(ThreadConfinedArena.CURRENT.get(), not(sameInstance(outer)));
                inner.set(ThreadConfinedArena.allocate(8, 8));
                return null;
            });
            assertThat(inner.get().scope().isAlive(), is(false));
            assertThat(ThreadConfinedArena.CURRENT.get(), sameInstance(outer));
            return null;
        });
        assertThat(ThreadConfinedArena.CURRENT.get(), nullValue());
    }

    public void testScopeRestoredWhenActionThrows() {
        IOException e = expectThrows(
            IOException.class,
            () -> ThreadConfinedArena.withConfinedArena(() -> { throw new IOException("boom"); })
        );
        assertThat(e.getMessage(), equalTo("boom"));
        assertThat(ThreadConfinedArena.CURRENT.get(), nullValue());
    }
}
