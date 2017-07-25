/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.jdbc.util.Version;

public class VersionTests extends ESTestCase {
    public void testVersionIsUnknownWithoutAJar() {
        // We aren't running in a jar so we have a bunch of "Unknown"
        assertEquals("Unknown", Version.versionNumber());
        assertEquals("Unknown", Version.versionHashShort());
        assertEquals(0, Version.versionMajor());
        assertEquals(0, Version.versionMinor());
    }
}
