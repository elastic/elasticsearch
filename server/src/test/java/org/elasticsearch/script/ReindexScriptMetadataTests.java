/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.script.field.Op;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

public class ReindexScriptMetadataTests extends ESTestCase {
    public void testIndex() {
        ReindexScript.Metadata md = new ReindexScript.Metadata("myIndex", "myId", 5L, "myRouting", Op.INDEX, Collections.emptyMap());
        assertEquals("myIndex", md.getIndex());
        NullPointerException npe = expectThrows(NullPointerException.class, () -> md.setIndex(null));
        assertEquals("destination index must be non-null", npe.getMessage());
        assertEquals("myIndex", md.getIndex());
        md.getCtx().put("_index", null);
        npe = expectThrows(NullPointerException.class, () -> md.setIndex(null));
        assertEquals("destination index must be non-null", npe.getMessage());
        md.setIndex("myIndex2");
        assertEquals("myIndex2", md.getIndex());
    }
}
