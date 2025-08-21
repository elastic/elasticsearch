/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.geoip;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.junit.rules.ExternalResource;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;

/**
 * This fixture is used to simulate a maxmind-provided server for downloading maxmind geoip database files from the
 * EnterpriseGeoIpDownloader. It can be used by integration tests so that they don't actually hit maxmind servers.
 */
public class EnterpriseGeoIpHttpFixture extends ExternalResource {

    private final List<String> maxmindDatabaseTypes;
    private final List<String> ipinfoDatabaseTypes;
    private HttpServer server;

    /*
     * The values in maxmindDatabaseTypes must be in DatabaseConfiguration.MAXMIND_NAMES, and the ipinfoDatabaseTypes
     * must be in DatabaseConfiguration.IPINFO_NAMES.
     */
    public EnterpriseGeoIpHttpFixture(List<String> maxmindDatabaseTypes, List<String> ipinfoDatabaseTypes) {
        this.maxmindDatabaseTypes = List.copyOf(maxmindDatabaseTypes);
        this.ipinfoDatabaseTypes = List.copyOf(ipinfoDatabaseTypes);
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/";
    }

    @Override
    protected void before() throws Throwable {
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);

        // for expediency reasons, it is handy to have this test fixture be able to serve the dual purpose of actually stubbing
        // out the download protocol for downloading files from maxmind (see the looped context creation after this stanza), as
        // we as to serve an empty response for the geoip.elastic.co service here
        this.server.createContext("/", exchange -> {
            String response = "[]"; // an empty json array
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(response.getBytes(StandardCharsets.UTF_8));
            }
        });

        // register the file types for the download fixture
        for (String databaseType : maxmindDatabaseTypes) {
            createContextForMaxmindDatabase(databaseType);
        }
        for (String databaseType : ipinfoDatabaseTypes) {
            createContextForIpinfoDatabase(databaseType);
        }

        server.start();
    }

    private static InputStream fixtureStream(String name) {
        return Objects.requireNonNull(GeoIpHttpFixture.class.getResourceAsStream(name));
    }

    private void createContextForMaxmindDatabase(String databaseType) {
        this.server.createContext("/" + databaseType + "/download", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            if (exchange.getRequestURI().toString().contains("sha256")) {
                MessageDigest sha256 = MessageDigests.sha256();
                try (InputStream in = fixtureStream("/geoip-fixture/" + databaseType + ".tgz")) {
                    sha256.update(in.readAllBytes());
                }
                exchange.getResponseBody()
                    .write(
                        (MessageDigests.toHexString(sha256.digest()) + "  " + databaseType + "_20240709.tar.gz").getBytes(
                            StandardCharsets.UTF_8
                        )
                    );
            } else {
                try (
                    OutputStream out = exchange.getResponseBody();
                    InputStream in = fixtureStream("/geoip-fixture/" + databaseType + ".tgz")
                ) {
                    in.transferTo(out);
                }
            }
            exchange.getResponseBody().close();
        });
    }

    private void createContextForIpinfoDatabase(String databaseType) {
        this.server.createContext("/free/" + databaseType + ".mmdb", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            if (exchange.getRequestURI().toString().contains("checksum")) {
                MessageDigest md5 = MessageDigests.md5();
                try (InputStream in = fixtureStream("/ipinfo-fixture/ip_" + databaseType + "_sample.mmdb")) {
                    md5.update(in.readAllBytes());
                }
                exchange.getResponseBody().write(Strings.format("""
                    { "checksums": { "md5": "%s" } }
                    """, MessageDigests.toHexString(md5.digest())).getBytes(StandardCharsets.UTF_8));
            } else {
                try (
                    OutputStream out = exchange.getResponseBody();
                    InputStream in = fixtureStream("/ipinfo-fixture/ip_" + databaseType + "_sample.mmdb")
                ) {
                    in.transferTo(out);
                }
            }
            exchange.getResponseBody().close();
        });
    }

    @Override
    protected void after() {
        server.stop(0);
    }
}
