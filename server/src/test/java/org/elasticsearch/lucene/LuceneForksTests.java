/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.apache.lucene.util.Version;
import org.elasticsearch.lucene.LuceneForks.Fork;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the {@link LuceneForks} class itself
 */
public class LuceneForksTests extends ESTestCase {

    public void testExpiry() {
        Fork fork = LuceneForks.expiring("a.b.XFoo", "apache/lucene#1", 10, 6, 0, "Delete XFoo.");
        assertFalse(fork.isExpired(Version.fromBits(10, 5, 2)));
        assertTrue(fork.isExpired(Version.fromBits(10, 6, 0)));
        assertTrue(fork.isExpired(Version.fromBits(11, 0, 0)));
        String message = fork.expiryMessage(Version.fromBits(10, 6, 0));
        assertThat(message, containsString("a.b.XFoo"));
        assertThat(message, containsString("apache/lucene#1"));
        assertThat(message, containsString("Delete XFoo."));
    }

    public void testWorkaroundExpiry() {
        Fork workaround = LuceneForks.workaround("a.b.Baz", "apache/lucene#2", 10, 6, 0, "Remove the check in Baz.");
        assertEquals(LuceneForks.Kind.WORKAROUND, workaround.kind());
        assertFalse(workaround.isExpired(Version.fromBits(10, 5, 2)));
        assertTrue(workaround.isExpired(Version.fromBits(10, 6, 0)));
        String message = workaround.expiryMessage(Version.fromBits(10, 6, 0));
        assertThat(message, containsString("workaround in a.b.Baz"));
        assertThat(message, containsString("Remove the check in Baz."));
    }

    public void testCompatibilityForksNeverExpire() {
        Fork fork = LuceneForks.compatibility("a.b.XBar", "reason");
        assertThat(fork.fixedIn(), equalTo(null));
        assertFalse(fork.isExpired(Version.fromBits(99, 0, 0)));
    }

    public void testPermanentForksNeverExpire() {
        Fork fork = LuceneForks.permanent("a.b.XBar", "reason");
        assertThat(fork.fixedIn(), equalTo(null));
        assertFalse(fork.isExpired(Version.fromBits(99, 0, 0)));
    }
}
