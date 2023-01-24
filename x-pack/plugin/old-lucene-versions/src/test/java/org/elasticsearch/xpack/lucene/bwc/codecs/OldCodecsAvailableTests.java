/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

public class OldCodecsAvailableTests extends ESTestCase {

    /**
     * Reminder to add Lucene BWC codecs under {@link org.elasticsearch.xpack.lucene.bwc.codecs} whenever Elasticsearch is upgraded
     * to the next major Lucene version.
     */
    public void testLuceneBWCCodecsAvailable() {
        assertEquals("Add Lucene BWC codecs for Elasticsearch version 7", 8, Version.CURRENT.major);
    }

}
