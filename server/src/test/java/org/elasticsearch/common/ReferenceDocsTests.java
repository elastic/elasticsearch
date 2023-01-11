/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.ReferenceDocs.getVersionComponent;

public class ReferenceDocsTests extends ESTestCase {

    public void testVersionComponent() {
        // Snapshot x.y.0 versions are unreleased so link to master
        assertEquals("master", getVersionComponent(Version.V_8_7_0, true));

        // Snapshot x.y.z versions with z>0 mean that x.y.0 is released so have a properly versioned docs link
        assertEquals("8.5", getVersionComponent(Version.V_8_5_1, true));

        // Non-snapshot versions are to be released so have a properly versioned docs link
        assertEquals("8.7", getVersionComponent(Version.V_8_7_0, false));
    }
}
