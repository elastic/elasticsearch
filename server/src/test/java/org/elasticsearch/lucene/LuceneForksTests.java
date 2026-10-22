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

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class LuceneForksTests extends ESTestCase {

    /**
     * Fails when we upgrade to a Lucene release that contains the upstream fix for a registered fork.
     * If this fails, the message says which fork to delete.
     */
    public void testNoForkHasExpired() {
        for (Fork fork : LuceneForks.ALL) {
            assertFalse(fork.expiryMessage(Version.LATEST), fork.isExpired(Version.LATEST));
        }
    }

    /** Fails if a registered fork was renamed or deleted without updating the registry. */
    public void testRegisteredForksExist() throws ClassNotFoundException {
        Set<String> seen = new HashSet<>();
        for (Fork fork : LuceneForks.ALL) {
            assertTrue("duplicate registry entry " + fork.className(), seen.add(fork.className()));
            Class<?> clazz = Class.forName(fork.className());
            assertThat(clazz.getSimpleName(), startsWith("X"));
        }
    }

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

    public void testPermanentForksNeverExpire() {
        Fork fork = LuceneForks.permanent("a.b.XBar", "reason");
        assertThat(fork.fixedIn(), equalTo(null));
        assertFalse(fork.isExpired(Version.fromBits(99, 0, 0)));
    }
}
