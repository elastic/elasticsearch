/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import static org.elasticsearch.common.logging.TestThreadInfoPatternConverter.threadInfo;

public class TestThreadInfoPatternConverterTests extends ESTestCase {
    private static String suiteInfo;

    @BeforeClass
    public static void captureSuiteInfo() {
        suiteInfo = threadInfo(Thread.currentThread().getName());
    }

    public void testThreadInfo() {
        // Threads that are part of a node get the node name
        String nodeName = randomAlphaOfLength(5);
        String threadName = EsExecutors.threadName(nodeName, randomAlphaOfLength(20)) + "[T#" + between(0, 1000) + "]";
        assertEquals(nodeName, threadInfo(threadName));

        // Test threads get the test name
        assertEquals(getTestName(), threadInfo(Thread.currentThread().getName()));

        // Suite initialization gets "suite"
        assertEquals("suite", suiteInfo);

        // And stuff that doesn't match anything gets wrapped in [] so we can see it
        String unmatched = randomAlphaOfLength(5);
        assertEquals("[" + unmatched + "]", threadInfo(unmatched));
    }
}
