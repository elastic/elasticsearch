/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package fixture.geoip;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.hash.MessageDigests;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;

/**
 * This fixture is used to simulate a maxmind-provided server for downloading maxmind geoip database files from the
 * EnterpriseGeoIpDownloader. It can be used by integration tests so that they don't actually hit maxmind servers.
 */
public class EnterpriseGeoIpHttpFixture extends ExternalResource {

    private final Path source;
    private final String[] databaseTypes;
    private HttpServer server;

    /*
     * The values in databaseTypes must be in DatabaseConfiguration.MAXMIND_NAMES, and must be one of the databases copied in the
     * copyFiles method of thisi class.
     */
    public EnterpriseGeoIpHttpFixture(String... databaseTypes) {
        this.databaseTypes = databaseTypes;
        try {
            this.source = Files.createTempDirectory("source");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/";
    }

    @Override
    protected void before() throws Throwable {
        copyFiles();
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);

        // for expediency reasons, it is handy to have this test fixture be able to serve the dual purpose of actually stubbing
        // out the download protocol for downloading files from maxmind (see the looped context creation after this stanza), as
        // we as to serve an empty response for the geoip.elastic.co service here
        this.server.createContext("/", exchange -> {
            String response = "[]"; // an empty json array
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        });

        // register the file types for the download fixture
        for (String databaseType : databaseTypes) {
            createContextForEnterpriseDatabase(databaseType);
        }

        server.start();
    }

    private void createContextForEnterpriseDatabase(String databaseType) {
        this.server.createContext("/" + databaseType + "/download", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            if (exchange.getRequestURI().toString().contains("sha256")) {
                MessageDigest sha256 = MessageDigests.sha256();
                try (InputStream inputStream = GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/" + databaseType + ".tgz")) {
                    sha256.update(inputStream.readAllBytes());
                }
                exchange.getResponseBody()
                    .write(
                        (MessageDigests.toHexString(sha256.digest()) + "  " + databaseType + "_20240709.tar.gz").getBytes(
                            StandardCharsets.UTF_8
                        )
                    );
            } else {
                try (
                    OutputStream outputStream = exchange.getResponseBody();
                    InputStream inputStream = GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/" + databaseType + ".tgz")
                ) {
                    inputStream.transferTo(outputStream);
                }
            }
            exchange.getResponseBody().close();
        });
    }

    @Override
    protected void after() {
        server.stop(0);
    }

    private void copyFiles() throws Exception {
        for (String databaseType : databaseTypes) {
            Files.copy(
                GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/GeoIP2-City.tgz"),
                source.resolve(databaseType + ".tgz"),
                StandardCopyOption.REPLACE_EXISTING
            );
        }
    }
}
