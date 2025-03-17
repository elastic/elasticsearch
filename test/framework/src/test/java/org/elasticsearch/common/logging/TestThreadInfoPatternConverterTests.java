/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import static org.elasticsearch.common.logging.TestThreadInfoPatternConverter.threadInfo;
import static org.hamcrest.Matchers.equalTo;

public class TestThreadInfoPatternConverterTests extends ESTestCase {
    private static String suiteInfo;

    @BeforeClass
    public static void captureSuiteInfo() {
        suiteInfo = threadInfo(Thread.currentThread().getName());
    }

    public void testElasticsearchThreadInfo() {
        // Threads that are part of a node get the node name
        var nodeName = randomAlphaOfLength(5);
        var prefix = randomAlphaOfLength(20);
        var thread = "T#" + between(0, 1000);
        var threadName = EsExecutors.threadName(nodeName, prefix) + "[" + thread + "]";
        assertThat(threadInfo(threadName), equalTo(nodeName + "][" + prefix + "][" + thread));
    }

    public void testCaseNameThreadInfo() {
        assertEquals(getTestName(), threadInfo(Thread.currentThread().getName()));
    }

    public void testSuiteThreadInfo() {
        assertEquals("suite", suiteInfo);
    }

    public void testUnmatchedThreadInfo() {
        String unmatched = randomAlphaOfLength(5);
        assertEquals("[" + unmatched + "]", threadInfo(unmatched));
    }
}
