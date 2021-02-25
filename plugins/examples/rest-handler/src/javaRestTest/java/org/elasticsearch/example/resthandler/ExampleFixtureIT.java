/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
