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
import org.junit.rules.ExternalResource;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class GeoIpHttpFixture extends ExternalResource {

    private final Path source;
    private final Path target;
    private final boolean enabled;
    private HttpServer server;

    public GeoIpHttpFixture(boolean enabled) {
        this.enabled = enabled;
        try {
            this.source = Files.createTempDirectory("source");
            this.target = Files.createTempDirectory("target");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/";
    }

    @Override
    protected void before() throws Throwable {
        if (enabled) {
            copyFiles();
            String data = new String(
                GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/data.json").readAllBytes(),
                StandardCharsets.UTF_8
            );
            this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
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
                    OutputStream outputStream = exchange.getResponseBody();
                    InputStream db = GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture" + dbName)
                ) {
                    db.transferTo(outputStream);
                }
            });
            this.server.createContext("/cli", exchange -> {
                String fileName = exchange.getRequestURI().getPath().replaceAll(".*/cli/", "");
                Path targetPath = target.resolve(fileName);
                if (Files.isRegularFile(targetPath)) {
                    try (OutputStream outputStream = exchange.getResponseBody(); InputStream db = Files.newInputStream(targetPath)) {
                        exchange.sendResponseHeaders(200, 0);
                        db.transferTo(outputStream);
                    } catch (Exception e) {
                        exchange.sendResponseHeaders(500, 0);
                        exchange.getResponseBody().close();
                    }
                } else {
                    exchange.sendResponseHeaders(404, 0);
                    exchange.getResponseBody().close();
                }
            });
            server.start();
        }
    }

    @Override
    protected void after() {
        if (enabled) {
            server.stop(0);
        }
    }

    private void copyFiles() throws Exception {
        Files.copy(GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/GeoLite2-ASN.tgz"), source.resolve("GeoLite2-ASN.tgz"));
        Files.copy(GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/GeoLite2-City.mmdb"), source.resolve("GeoLite2-City.mmdb"));
        Files.copy(
            GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/GeoLite2-Country.mmdb"),
            source.resolve("GeoLite2-Country.mmdb")
        );

        new GeoIpCli().main(
            new String[] { "-s", source.toAbsolutePath().toString(), "-t", target.toAbsolutePath().toString() },
            Terminal.DEFAULT,
            null
        );
    }
}
