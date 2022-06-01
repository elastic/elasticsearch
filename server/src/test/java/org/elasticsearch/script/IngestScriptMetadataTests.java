/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class IngestScriptMetadataTests extends ESTestCase {
    public void testVersionType() {
        Map<String, Object> ctx = new HashMap<>();
        IngestScript.Metadata m = new IngestScript.Metadata(ctx, null);
        assertNull(m.getVersionType());
        for (VersionType vt : VersionType.values()) {
            m.setVersionType(vt);
            assertEquals(VersionType.toString(vt), ctx.get("_version_type"));
        }

        for (VersionType vt : VersionType.values()) {
            ctx.put("_version_type", VersionType.toString(vt));
            assertEquals(vt, m.getVersionType());
        }

        m.setVersionType(null);
        assertNull(m.getVersionType());
    }
}
