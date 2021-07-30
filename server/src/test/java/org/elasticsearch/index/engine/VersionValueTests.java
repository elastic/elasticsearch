/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.RamUsageTester;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

public class VersionValueTests extends ESTestCase {

    public void testIndexRamBytesUsed() {
        Translog.Location translogLoc = null;
        if (randomBoolean()) {
            translogLoc = new Translog.Location(randomNonNegativeLong(), randomNonNegativeLong(), randomInt());
        }
        IndexVersionValue versionValue = new IndexVersionValue(translogLoc, randomLong(), randomLong(), randomLong());
        assertEquals(RamUsageTester.sizeOf(versionValue), versionValue.ramBytesUsed());
    }

    public void testDeleteRamBytesUsed() {
        DeleteVersionValue versionValue = new DeleteVersionValue(randomLong(), randomLong(), randomLong(), randomLong());
        assertEquals(RamUsageTester.sizeOf(versionValue), versionValue.ramBytesUsed());
    }

}
