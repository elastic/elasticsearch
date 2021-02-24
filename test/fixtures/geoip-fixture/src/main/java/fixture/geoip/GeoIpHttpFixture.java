/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package fixture.geoip;

import com.sun.net.httpserver.HttpServer;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class GeoIpHttpFixture {

    private final HttpServer server;

    GeoIpHttpFixture(final String[] args) throws Exception {
        String rawData = new String(GeoIpHttpFixture.class.getResourceAsStream("/data.json").readAllBytes(), StandardCharsets.UTF_8);
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(args[0]), Integer.parseInt(args[1])), 0);
        this.server.createContext("/", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            if (query.contains("elastic_geoip_service_tos=agree") == false) {
                exchange.sendResponseHeaders(400, 0);
                return;
            }
            String data = rawData.replace("endpoint", "http://" + exchange.getRequestHeaders().getFirst("Host"));
            exchange.sendResponseHeaders(200, data.length());
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()))) {
                writer.write(data);
            }
        });
        this.server.createContext("/db.mmdb.gz", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                GeoIpHttpFixture.class.getResourceAsStream("/GeoIP2-City-Test.mmdb.gz").transferTo(outputStream);
            }
        });
    }

    final void start() throws Exception {
        try {
            server.start();
            // wait to be killed
            Thread.sleep(Long.MAX_VALUE);
        } finally {
            server.stop(0);
        }
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 2) {
            throw new IllegalArgumentException("GeoIpHttpFixture expects 2 arguments [address, port]");
        }
        final GeoIpHttpFixture fixture = new GeoIpHttpFixture(args);
        fixture.start();
    }
}
