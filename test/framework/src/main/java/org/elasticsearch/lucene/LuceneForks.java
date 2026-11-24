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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.assertFalse;
import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.test.ESTestCase.assertTrue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Class for managing registries of Elasticsearch's forks of Lucene classes (the {@code X*} classes).
 *
 * <p>Every fork that exists to carry an upstream bug fix must be registered here with the Lucene
 * release that contains the fix. Lucene fork test classes for each module fails once the Lucene version
 * we build against reaches that release, which is the signal to delete the fork.
 * <p>Forks that are intentional
 * extensions rather than bug fixes are registered with {@link #permanent} so that they can still be
 * found and checked for existence during Lucene upgrades.
 * <p>Forks that are maintained for backwards compatibility are registered with {@link #compatibility};
 * the affected classes should also have an {@code UpdateForVX} annotation on them.
 *
 * <p>Note that these lists may not be exhaustive - if you find an unregistered fork while looking at
 * other code, please register it so that we can track it.
 *
 * <p>Workarounds for Lucene bugs that don't need a forked class are also registered with
 * {@link #workaround}, so that we are reminded to remove the workaround once the fix has been
 * released.
 */
public final class LuceneForks {

    public enum Kind {
        /** A copy of a Lucene class, named {@code X*}. */
        FORK("fork"),
        /** Elasticsearch code that works around a Lucene bug without copying a Lucene class. */
        WORKAROUND("workaround");

        private final String description;

        Kind(String description) {
            this.description = description;
        }
    }

    /**
     * A forked class, or a workaround for a Lucene bug.
     *
     * @param kind           whether this is a fork or a workaround
     * @param className      fully qualified name of the fork, or of the class containing the workaround; checked to exist,
     *                       so renames must update the registry
     * @param upstreamIssue  the apache/lucene issue or PR motivating the fork, or a description for permanent forks
     * @param fixedIn        first Lucene release containing the upstream fix, or {@code null} if the fork is permanent
     * @param removalNote    what to do when the fork or workaround expires
     */
    public record Fork(Kind kind, String className, String upstreamIssue, Version fixedIn, String removalNote) {

        public boolean isExpired(Version luceneVersion) {
            return fixedIn != null && luceneVersion.onOrAfter(fixedIn);
        }

        public String expiryMessage(Version luceneVersion) {
            return "Lucene "
                + luceneVersion
                + " contains the fix for "
                + upstreamIssue
                + " ("
                + fixedIn
                + "); the "
                + kind.description
                + " in "
                + className
                + " is no longer needed. "
                + removalNote
                + " Then remove it from LuceneForks.";
        }
    }

    /**
     * Checks {@code forks} for any that have expired, and throws {@link AssertionError} if they have
     */
    public static void checkForksForExpiry(Collection<Fork> forks) {
        for (LuceneForks.Fork fork : forks) {
            assertFalse(fork.expiryMessage(Version.LATEST), fork.isExpired(Version.LATEST));
        }
    }

    /**
     * Checks that {@code forks} are still valid, and throws {@link AssertionError} if any classes no longer exist
     */
    public static void checkForksAreValid(Collection<Fork> forks) {
        Set<String> seen = new HashSet<>();
        for (LuceneForks.Fork fork : forks) {
            try {
                assertTrue("duplicate registry entry " + fork.className(), seen.add(fork.className()));
                Class<?> clazz = Class.forName(fork.className());
                if (fork.kind() == LuceneForks.Kind.FORK) {
                    assertThat(clazz.getSimpleName(), startsWith("X"));
                }
            } catch (ClassNotFoundException e) {
                throw new AssertionError("Forked class " + fork.className() + " does not exist", e);
            }
        }
    }

    static Fork expiring(String className, String upstreamIssue, int major, int minor, int bugfix, String removalNote) {
        return new Fork(Kind.FORK, className, upstreamIssue, Version.fromBits(major, minor, bugfix), removalNote);
    }

    /**
     * Registers a workaround for a Lucene bug that lives in {@code className}.
     */
    static Fork workaround(String className, String upstreamIssue, int major, int minor, int bugfix, String removalNote) {
        return new Fork(Kind.WORKAROUND, className, upstreamIssue, Version.fromBits(major, minor, bugfix), removalNote);
    }

    /**
     * Registers a class that is needed to maintain backwards compatibility in some way
     */
    static Fork compatibility(String className, String removalNote) {
        return new Fork(Kind.FORK, className, null, null, removalNote);
    }

    static Fork permanent(String className, String reason) {
        return new Fork(Kind.FORK, className, reason, null, "This fork is intentional and does not expire.");
    }

    private LuceneForks() {}
}
