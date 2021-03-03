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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class GeoIpHttpFixture {

    private final HttpServer server;

    GeoIpHttpFixture(final String[] args) throws Exception {
        byte[] bytes = new byte[4096];
        int read;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (InputStream is = GeoIpHttpFixture.class.getResourceAsStream("/data.json")) {
            while ((read = is.read(bytes)) != -1) {
                byteArrayOutputStream.write(bytes, 0, read);
            }
        }
        String rawData = new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(args[0]), Integer.parseInt(args[1])), 0);
        this.server.createContext("/", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            if (query == null || query.contains("elastic_geoip_service_tos=agree") == false) {
                exchange.sendResponseHeaders(400, 0);
                exchange.getResponseBody().close();
                return;
            }
            String data = rawData.replace("endpoint", "http://" + exchange.getRequestHeaders().getFirst("Host"));
            exchange.sendResponseHeaders(200, data.length());
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()))) {
                writer.write(data);
            }
        });
        this.server.createContext("/db", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            String dbName = exchange.getRequestURI().getPath().replaceAll(".*/db", "");
            try (InputStream inputStream = GeoIpHttpFixture.class.getResourceAsStream(dbName);
                 OutputStream outputStream = exchange.getResponseBody()) {
                int read2;
                while ((read2 = inputStream.read(bytes)) != -1) {
                    outputStream.write(bytes, 0, read2);
                }
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
