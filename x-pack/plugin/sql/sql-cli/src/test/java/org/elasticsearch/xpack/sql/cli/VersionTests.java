/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

public class VersionTests extends SqlCliTestCase {
    public void testVersionIsCurrent() {
        /* This test will only work properly in gradle because in gradle we run the tests
         * using the jar. */
        assertEquals(SqlVersion.fromString(Build.current().version()), ClientVersion.CURRENT);
    }

}
