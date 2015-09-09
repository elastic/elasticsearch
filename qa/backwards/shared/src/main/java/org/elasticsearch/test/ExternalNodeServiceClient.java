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

package org.elasticsearch.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Client for the ExternalNodeService which is a Java process that forks and
 * manages Elasticsearch subprocesses for testing. This class uses plain old
 * socket communication because ExternalNodeSerice's protocol is simple,
 * streaming, and synchronous.
 */
public class ExternalNodeServiceClient {
    private final int port;

    /**
     * Build the client to talk to a server at the default port.
     */
    public ExternalNodeServiceClient() {
        this(ExternalNodeService.DEFAULT_PORT);
    }

    public ExternalNodeServiceClient(int port) {
        this.port = port;
    }

    public int start(final String args) {
        return Integer.parseInt(interact("start " + args, "bound to \\[localhost:(\\d+)\\]").group(1), 10);
    }

    public void stop(int esPort) {
        interact("stop " + esPort, "killed");
    }

    public void shutdownService() {
        interact("shutdown", ExternalNodeService.SHUTDOWN_MESSAGE);
    }

    private Matcher interact(String message, String expectedResponse) {
        Pattern expected = Pattern.compile(expectedResponse);
        try (Socket s = new Socket(InetAddress.getByName("localhost"), port)) {
            s.setTcpNoDelay(true);
            s.setSoTimeout(20000);
            try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8))) {
                out.write(message + '\n');
                out.flush();
                try (BufferedReader response = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
                    List<String> collected = new ArrayList<>();
                    String line;
                    while ((line = response.readLine()) != null) {
                        collected.add(line);
                        Matcher m = expected.matcher(line);
                        if (m.matches()) {
                            return m;
                        }
                    }
                    throw new IllegalStateException("Failed to start elasticserch. Responses: " + collected);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
