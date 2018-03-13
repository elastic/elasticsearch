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

package org.elasticsearch.qa.die_with_dignity;

import org.apache.http.ConnectionClosedException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class DieWithDignityIT extends ESRestTestCase {

    public void testDieWithDignity() throws Exception {
        // deleting the PID file prevents stopping the cluster from failing since it occurs if and only if the PID file exists
        final Path pidFile = PathUtils.get(System.getProperty("pidfile"));
        final List<String> pidFileLines = Files.readAllLines(pidFile);
        assertThat(pidFileLines, hasSize(1));
        final int pid = Integer.parseInt(pidFileLines.get(0));
        Files.delete(pidFile);
        expectThrows(ConnectionClosedException.class, () -> client().performRequest("GET", "/_die_with_dignity"));

        // the Elasticsearch process should die and disappear from the output of jps
        assertBusy(() -> {
            final String jpsPath = PathUtils.get(System.getProperty("runtime.java.home"), "bin/jps").toString();
            final Process process = new ProcessBuilder().command(jpsPath).start();
            assertThat(process.waitFor(), equalTo(0));
            try (InputStream is = process.getInputStream();
                 BufferedReader in = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
                String line;
                while ((line = in.readLine()) != null) {
                    final int currentPid = Integer.parseInt(line.split("\\s+")[0]);
                    assertThat(line, pid, not(equalTo(currentPid)));
                }
            }
        });

        // parse the logs and ensure that Elasticsearch died with the expected cause
        final List<String> lines = Files.readAllLines(PathUtils.get(System.getProperty("log")));

        final Iterator<String> it = lines.iterator();

        boolean fatalErrorOnTheNetworkLayer = false;
        boolean fatalErrorInThreadExiting = false;

        while (it.hasNext() && (fatalErrorOnTheNetworkLayer == false || fatalErrorInThreadExiting == false)) {
            final String line = it.next();
            if (line.contains("fatal error on the network layer")) {
                fatalErrorOnTheNetworkLayer = true;
            } else if (line.matches(".*\\[ERROR\\]\\[o.e.b.ElasticsearchUncaughtExceptionHandler\\] \\[node-0\\]"
                    + " fatal error in thread \\[Thread-\\d+\\], exiting$")) {
                fatalErrorInThreadExiting = true;
                assertTrue(it.hasNext());
                assertThat(it.next(), equalTo("java.lang.OutOfMemoryError: die with dignity"));
            }
        }

        assertTrue(fatalErrorOnTheNetworkLayer);
        assertTrue(fatalErrorInThreadExiting);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // as the cluster is dead its state can not be wiped successfully so we have to bypass wiping the cluster
        return true;
    }

}
