/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.stream.IntStream;

public class AutoReadSyncTests extends ESTestCase {

    Channel chan;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        chan = new EmbeddedChannel();
    }

    AutoReadSync.Handle getHandle() {
        return AutoReadSync.getHandle(chan);
    }

    public void testToggleSetAutoRead() {
        var autoRead = getHandle();
        assertTrue("must be enabled by default", autoRead.isEnabled());

        autoRead.disable();
        assertFalse("must disable handle", autoRead.isEnabled());
        assertFalse("must turn off chan autoRead", chan.config().isAutoRead());

        autoRead.enable();
        assertTrue("must enable handle", autoRead.isEnabled());
        assertTrue("must turn on chan autoRead", chan.config().isAutoRead());

        autoRead.disable();
        autoRead.release();
        assertTrue("must turn on chan autoRead on release", chan.config().isAutoRead());
    }

    public void testAnyToggleDisableAutoRead() {
        var handles = IntStream.range(0, 100).mapToObj(i -> getHandle()).toList();
        handles.forEach(AutoReadSync.Handle::enable);
        handles.get(between(0, 100)).disable();
        assertFalse(chan.config().isAutoRead());
    }

    public void testNewHandleDoesNotChangeAutoRead() {
        var handle1 = getHandle();

        handle1.disable();
        assertFalse(chan.config().isAutoRead());
        getHandle();
        assertFalse("acquiring new handle should enable autoRead", chan.config().isAutoRead());

        handle1.enable();
        assertTrue(chan.config().isAutoRead());
        getHandle();
        assertTrue("acquiring new handle should not disable autoRead", chan.config().isAutoRead());
    }

    public void testAllTogglesEnableAutoRead() {
        // mix-in acquire/release
        var handles = new HashSet<AutoReadSync.Handle>();
        IntStream.range(0, 100).mapToObj(i -> getHandle()).forEach(h -> {
            h.disable();
            handles.add(h);
        });
        assertFalse(chan.config().isAutoRead());

        var toRelease = between(1, 98); // release some but not all
        var releasedHandles = handles.stream().limit(toRelease).toList();
        releasedHandles.forEach(h -> {
            h.release();
            handles.remove(h);
        });
        assertFalse("releasing some but not all handles should not enable autoRead", chan.config().isAutoRead());

        var lastHandle = getHandle();
        lastHandle.disable();
        for (var handle : handles) {
            handle.enable();
            assertFalse("should not enable autoRead until lastHandle is enabled", chan.config().isAutoRead());
        }
        lastHandle.enable();
        assertTrue(chan.config().isAutoRead());
    }

}
