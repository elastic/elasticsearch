/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package fixture.geoip;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.geoip.GeoIpCli;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class GeoIpHttpFixture {

    private final HttpServer server;

    GeoIpHttpFixture(final String[] args) throws Exception {
        copyFiles();
        byte[] bytes = new byte[4096];
        int read;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (InputStream is = GeoIpHttpFixture.class.getResourceAsStream("/data.json")) {
            while ((read = is.read(bytes)) != -1) {
                byteArrayOutputStream.write(bytes, 0, read);
            }
        }
        String data = new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(args[0]), Integer.parseInt(args[1])), 0);
        this.server.createContext("/", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            if (query == null || query.contains("elastic_geoip_service_tos=agree") == false) {
                exchange.sendResponseHeaders(400, 0);
                exchange.getResponseBody().close();
                return;
            }
            exchange.sendResponseHeaders(200, data.length());
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()))) {
                writer.write(data);
            }
        });
        this.server.createContext("/db", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            String dbName = exchange.getRequestURI().getPath().replaceAll(".*/db", "");
            try (
                InputStream inputStream = GeoIpHttpFixture.class.getResourceAsStream(dbName);
                OutputStream outputStream = exchange.getResponseBody()
            ) {
                int read2;
                while ((read2 = inputStream.read(bytes)) != -1) {
                    outputStream.write(bytes, 0, read2);
                }
            }
        });
        this.server.createContext("/cli", exchange -> {
            String fileName = exchange.getRequestURI().getPath().replaceAll(".*/cli/", "");
            Path target = new File("target").toPath().resolve(fileName);
            if (Files.isRegularFile(target)) {
                try (OutputStream outputStream = exchange.getResponseBody(); InputStream db = Files.newInputStream(target)) {
                    exchange.sendResponseHeaders(200, 0);
                    int read3;
                    while ((read3 = db.read(bytes)) != -1) {
                        outputStream.write(bytes, 0, read3);
                    }
                } catch (Exception e) {
                    exchange.sendResponseHeaders(500, 0);
                    exchange.getResponseBody().close();
                }
            } else {
                exchange.sendResponseHeaders(404, 0);
                exchange.getResponseBody().close();
            }
        });
    }

    private void copyFiles() throws Exception {
        Path source = new File("source").toPath();
        Files.createDirectory(source);

        Path target = new File("target").toPath();
        Files.createDirectory(target);

        Files.copy(GeoIpHttpFixture.class.getResourceAsStream("/GeoLite2-ASN.tgz"), source.resolve("GeoLite2-ASN.tgz"));
        Files.copy(GeoIpHttpFixture.class.getResourceAsStream("/GeoLite2-City.mmdb"), source.resolve("GeoLite2-City.mmdb"));
        Files.copy(GeoIpHttpFixture.class.getResourceAsStream("/GeoLite2-Country.mmdb"), source.resolve("GeoLite2-Country.mmdb"));

        new GeoIpCli().main(
            new String[] { "-s", source.toAbsolutePath().toString(), "-t", target.toAbsolutePath().toString() },
            Terminal.DEFAULT
        );
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
