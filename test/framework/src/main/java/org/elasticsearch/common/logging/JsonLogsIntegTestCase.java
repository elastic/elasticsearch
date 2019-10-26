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
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

/**
 * Tests that extend this class verify that all json layout fields appear in the first few log lines after startup
 * Fields available upon process startup: <code>type</code>, <code>timestamp</code>, <code>level</code>, <code>component</code>,
 * <code>message</code>, <code>node.name</code>, <code>cluster.name</code>.
 * Whereas <code>node.id</code> and <code>cluster.uuid</code> are available later once the first clusterState has been received.
 *
 *
 * <code>node.name</code>, <code>cluster.name</code>, <code>node.id</code>, <code>cluster.uuid</code>
 * should not change across all log lines
 *
 * Note that this won't pass for nodes in clusters that don't have the node name defined in elasticsearch.yml <strong>and</strong> start
 * with DEBUG or TRACE level logging. Those nodes log a few lines before the node.name is set by <code>LogConfigurator.setNodeName</code>.
 */
public abstract class JsonLogsIntegTestCase extends ESRestTestCase {
    /**
     * Number of lines in the log file to check for the <code>node.name</code>, <code>node.id</code> or <code>cluster.uuid</code>. We don't
     * just check the entire log file because it could be quite long
     */
    private static final int LINES_TO_CHECK = 10;

    /**
     * The node name to expect in the log file.
     */
    protected abstract org.hamcrest.Matcher<String> nodeNameMatcher();

    /**
     * Open the log file. This is delegated to subclasses because the test
     * framework doesn't have permission to read from the log file but
     * subclasses can grant themselves that permission.
     */
    protected abstract BufferedReader openReader(Path logFile);

    public void testElementsPresentOnAllLinesOfLog() throws IOException {
        JsonLogLine firstLine = findFirstLine();
        assertNotNull(firstLine);

        try (Stream<JsonLogLine> stream = JsonLogsStream.from(openReader(getLogFile()))) {
            stream.limit(LINES_TO_CHECK)
                  .forEach(jsonLogLine -> {
                      assertThat(jsonLogLine.type(), not(isEmptyOrNullString()));
                      assertThat(jsonLogLine.timestamp(), not(isEmptyOrNullString()));
                      assertThat(jsonLogLine.level(), not(isEmptyOrNullString()));
                      assertThat(jsonLogLine.component(), not(isEmptyOrNullString()));
                      assertThat(jsonLogLine.message(), not(isEmptyOrNullString()));

                      // all lines should have the same nodeName and clusterName
                      assertThat(jsonLogLine.nodeName(), nodeNameMatcher());
                      assertThat(jsonLogLine.clusterName(), equalTo(firstLine.clusterName()));
                  });
        }
    }

    private JsonLogLine findFirstLine() throws IOException {
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(openReader(getLogFile()))) {
            return stream.findFirst()
                         .orElseThrow(() -> new AssertionError("no logs at all?!"));
        }
    }

    public void testNodeIdAndClusterIdConsistentOnceAvailable() throws IOException {
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(openReader(getLogFile()))) {
            Iterator<JsonLogLine> iterator = stream.iterator();

            JsonLogLine firstLine = null;
            while (iterator.hasNext()) {
                JsonLogLine jsonLogLine = iterator.next();
                if (jsonLogLine.nodeId() != null) {
                    firstLine = jsonLogLine;
                }
            }
            assertNotNull(firstLine);

            //once the nodeId and clusterId are received, they should be the same on remaining lines

            int i = 0;
            while (iterator.hasNext() && i++ < LINES_TO_CHECK) {
                JsonLogLine jsonLogLine = iterator.next();
                assertThat(jsonLogLine.nodeId(), equalTo(firstLine.nodeId()));
                assertThat(jsonLogLine.clusterUuid(), equalTo(firstLine.clusterUuid()));
            }
        }
    }

    @SuppressForbidden(reason = "PathUtils doesn't have permission to read this file")
    private Path getLogFile() {
        String logFileString = System.getProperty("tests.logfile");
        if (logFileString == null) {
            fail("tests.logfile must be set to run this test. It is automatically "
                + "set by gradle. If you must set it yourself then it should be the absolute path to the "
                + "log file.");
        }
        return Paths.get(logFileString);
    }
}
