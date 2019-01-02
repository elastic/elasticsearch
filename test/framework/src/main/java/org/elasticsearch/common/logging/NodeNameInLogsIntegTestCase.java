/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that extend this class verify that the node name appears in the first
 * few log lines on startup. Note that this won't pass for clusters that don't
 * the node name defined in elasticsearch.yml <strong>and</strong> start with
 * DEBUG or TRACE level logging. Those nodes log a few lines before they
 * resolve the node name.
 */
public abstract class NodeNameInLogsIntegTestCase extends ESRestTestCase {
    /**
     * Number of lines in the log file to check for the node name. We don't
     * just check the entire log file because it could be quite long and
     * exceptions don't include the node name.
     */
    private static final int LINES_TO_CHECK = 10;

    /**
     * The node name to expect in the logs file.
     */
    protected abstract org.hamcrest.Matcher<String> nodeNameMatcher();

    /**
     * Open the log file. This is delegated to subclasses because the test
     * framework doesn't have permission to read from the log file but
     * subclasses can grant themselves that permission.
     */
    protected abstract BufferedReader openReader(Path logFile);

    public void testNodeNameIsOnAllLinesOfLog() throws IOException {
        try (JsonLogs jsonLogs = new JsonLogs(openReader(getLogFile()))) {
            Iterator<JsonLogLine> it = jsonLogs.iterator();

            assertTrue("no logs at all?!", it.hasNext());
            JsonLogLine firstLine = it.next();


            String nodeName = firstLine.nodeName();
            assertThat(nodeName, nodeNameMatcher());


            for (int lineNumber = 1; lineNumber < LINES_TO_CHECK && it.hasNext(); lineNumber++) {
                JsonLogLine logLine = it.next();
                assertThat(logLine.nodeName(), equalTo(nodeName));
            }
        }
    }

    @SuppressForbidden(reason = "PathUtils doesn't have permission to read this file")
    private Path getLogFile() {
        String logFileString = System.getProperty("tests.logfile");
        if (null == logFileString) {
            fail("tests.logfile must be set to run this test. It is automatically "
                + "set by gradle. If you must set it yourself then it should be the absolute path to the "
                + "log file.");
        }
        return Paths.get(logFileString);
    }
}
