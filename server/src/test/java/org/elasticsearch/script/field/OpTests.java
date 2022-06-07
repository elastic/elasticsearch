/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OpTests extends ESTestCase {

    public void testFromString() {
        assertEquals(Op.UNKNOWN, Op.fromString(null));
        assertEquals(Op.UNKNOWN, Op.fromString("random"));
    }

    public void testEquivalenceWithOpType() {
        Set<Op> ops = new HashSet<>(List.of(Op.values()));
        for (DocWriteRequest.OpType ot : DocWriteRequest.OpType.values()) {
            ops.remove(Op.fromString(ot.getLowercase()));
        }
        assertTrue(ops.remove(Op.NOOP));
        assertTrue(ops.remove(Op.UNKNOWN));
        assertEquals(0, ops.size());
    }
}
