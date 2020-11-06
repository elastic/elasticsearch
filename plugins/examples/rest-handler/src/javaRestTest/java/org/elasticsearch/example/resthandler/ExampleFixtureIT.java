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

package org.elasticsearch.example.resthandler;

import org.elasticsearch.mocksocket.MockSocket;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasItems;

public class ExampleFixtureIT extends ESTestCase {

    public void testExample() throws Exception {
        final String externalAddress = System.getProperty("external.address");
        assertNotNull("External address must not be null", externalAddress);

        final URL url = new URL("http://" + externalAddress);
        final InetAddress address = InetAddress.getByName(url.getHost());
        try (
            Socket socket = new MockSocket(address, url.getPort());
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
        ) {
            writer.write("GET / HTTP/1.1\r\n");
            writer.write("Host: elastic.co\r\n\r\n");
            writer.flush();

            final List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            assertThat(lines, hasItems("HTTP/1.1 200 OK", "TEST"));
        }
    }
}
