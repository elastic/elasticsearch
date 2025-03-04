/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.aws.ec2;

import com.sun.net.httpserver.HttpServer;

import org.junit.rules.ExternalResource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

public class AwsEc2HttpFixture extends ExternalResource {

    private HttpServer server;

    private final Supplier<List<String>> transportAddressesSupplier;
    private final BiPredicate<String, String> authorizationPredicate;

    public AwsEc2HttpFixture(BiPredicate<String, String> authorizationPredicate, Supplier<List<String>> transportAddressesSupplier) {
        this.authorizationPredicate = Objects.requireNonNull(authorizationPredicate);
        this.transportAddressesSupplier = Objects.requireNonNull(transportAddressesSupplier);
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    public void stop(int delay) {
        server.stop(delay);
    }

    protected void before() throws Throwable {
        server = HttpServer.create(resolveAddress(), 0);
        server.createContext("/", new AwsEc2HttpHandler(authorizationPredicate, transportAddressesSupplier));
        server.start();
    }

    @Override
    protected void after() {
        stop(0);
    }

    private static InetSocketAddress resolveAddress() {
        try {
            return new InetSocketAddress(InetAddress.getByName("localhost"), 0);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
