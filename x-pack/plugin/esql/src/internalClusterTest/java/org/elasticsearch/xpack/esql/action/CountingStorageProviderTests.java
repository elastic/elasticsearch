/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import static org.hamcrest.Matchers.startsWith;

/**
 * Covers the two path rewrites {@link CountingStorageProvider} performs, on the spellings a Windows run produces.
 * <p>
 * The suites built on that provider exercise these on every listing, but only on whatever the host filesystem
 * hands them, so on POSIX they cannot distinguish a rewrite that keeps a Windows drive letter in the path from one
 * that leaves it in the authority -- the two produce the same string. These assertions feed in the delegate's
 * Windows spelling directly, so the defect is caught by a macOS or Linux run rather than by a Windows CI leg.
 */
public class CountingStorageProviderTests extends ESTestCase {

    private static final String WINDOWS_FILE_URI = "file:///C:/Users/jenkins/tmp/dir/a.csv";

    /**
     * A relabelled entry has to parse, and it has to begin with the location the dataset was registered under, or
     * the glob holds a prefix no listed key starts with and matches nothing.
     */
    public void testARelabelledWindowsEntryParsesAndKeepsTheRegisteredPrefix() {
        StoragePath relabelled = CountingStorageProvider.toCountScheme(StoragePath.of(WINDOWS_FILE_URI));

        assertEquals(CountingStorageProvider.SCHEME, relabelled.scheme());
        assertEquals("countfs:///C:/Users/jenkins/tmp/dir/a.csv", relabelled.toString());
        // What location(dir) spells for the directory holding that entry. Built here as a string rather than from a
        // Path, because a Windows ABSOLUTE path cannot be constructed on the platform this test usually runs on --
        // the same spelling parses there as a relative path, which is not what location(dir) would be handed.
        assertThat(relabelled.toString(), startsWith(CountingStorageProvider.SCHEME + ":///C:/Users/jenkins/tmp/dir"));
    }

    /** The two rewrites are inverses, so an entry handed to the query comes back to the delegate unchanged. */
    public void testTheSchemeRewritesAreInversesOnAWindowsPath() {
        StoragePath delegatePath = StoragePath.of(WINDOWS_FILE_URI);
        StoragePath roundTripped = CountingStorageProvider.toFileScheme(CountingStorageProvider.toCountScheme(delegatePath));

        assertEquals("file", roundTripped.scheme());
        assertEquals(delegatePath.localPath(), roundTripped.localPath());
        assertEquals("C:/Users/jenkins/tmp/dir/a.csv", roundTripped.localPath());
    }
}
