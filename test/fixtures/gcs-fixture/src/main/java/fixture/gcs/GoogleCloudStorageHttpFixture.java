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
package fixture.gcs;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class GoogleCloudStorageHttpFixture {

    private final HttpServer server;

    private GoogleCloudStorageHttpFixture(final String address, final int port,
                                          final String bucket, final String token) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(address), port), 0);
        server.createContext("/" + token, new FakeOAuth2HttpHandler());
        server.createContext("/", new GoogleCloudStorageHttpHandler(bucket));
    }

    private void start() throws Exception {
        try {
            server.start();
            // wait to be killed
            Thread.sleep(Long.MAX_VALUE);
        } finally {
            server.stop(0);
        }
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length != 4) {
            throw new IllegalArgumentException("GoogleCloudStorageHttpFixture expects 4 arguments [address, port, bucket, token]");
        }
        GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(args[0], Integer.parseInt(args[1]), args[2], args[3]);
        fixture.start();
    }
}
