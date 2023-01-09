/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.common.ReferenceDocs.getVersionComponent;
import static org.elasticsearch.common.ReferenceDocs.linksToVerify;

public class ReferenceDocsTests extends ESTestCase {

    public void testVersionComponent() {
        // Snapshot x.y.0 versions are unreleased so link to master
        assertEquals("master", getVersionComponent(Version.V_8_7_0, true));

        // Snapshot x.y.z versions with z>0 mean that x.y.0 is released so have a properly versioned docs link
        assertEquals("8.5", getVersionComponent(Version.V_8_5_1, true));

        // Non-snapshot versions are to be released so have a properly versioned docs link
        assertEquals("8.7", getVersionComponent(Version.V_8_7_0, false));
    }

    public void testLinksToVerify() {
        // Snapshot x.y.0 versions are unreleased so we link to master and these links are expected to exist
        assertFalse(linksToVerify(Version.V_8_7_0, true).isEmpty());

        // Snapshot x.y.z versions with z>0 mean that x.y.0 is released so links are expected to exist
        assertFalse(linksToVerify(Version.V_8_5_1, true).isEmpty());

        // Non-snapshot x.y.0 versions may not be released yet, and the docs are published on release, so we cannot verify these links
        assertTrue(linksToVerify(Version.V_8_7_0, false).isEmpty());
    }

    @AwaitsFix(bugUrl = "TODO")
    @SuppressForbidden(reason = "never executed")
    public void testDocsExist() throws IOException {
        // cannot run as a unit test due to security manager restrictions - TODO create a separate Gradle task for this
        for (ReferenceDocs docsLink : linksToVerify()) {
            try (var stream = new URL(docsLink.toString()).openStream()) {
                Streams.readFully(stream);
                // TODO also for URLs that contain a fragment id, verify that the fragment exists
            }
        }
    }
}
