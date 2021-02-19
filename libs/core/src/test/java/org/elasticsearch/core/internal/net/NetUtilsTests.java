/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.net;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.ESTestCase;

public class NetUtilsTests extends ESTestCase {

    public void testExtendedSocketOptions() {
        assumeTrue("JDK possibly not supported", Constants.JVM_NAME.contains("HotSpot") || Constants.JVM_NAME.contains("OpenJDK"));
        assumeTrue("Platform possibly not supported", IOUtils.LINUX || IOUtils.MAC_OS_X);
        assertNotNull(NetUtils.getTcpKeepIdleSocketOptionOrNull());
        assertNotNull(NetUtils.getTcpKeepIntervalSocketOptionOrNull());
        assertNotNull(NetUtils.getTcpKeepCountSocketOptionOrNull());
    }
}
