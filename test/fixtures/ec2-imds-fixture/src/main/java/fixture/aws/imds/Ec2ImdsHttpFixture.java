/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.aws.imds;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

public class Ec2ImdsHttpFixture extends ExternalResource {

    public static final String ENDPOINT_OVERRIDE_SYSPROP_NAME = "com.amazonaws.sdk.ec2MetadataServiceEndpointOverride";

    private final Ec2ImdsServiceBuilder ec2ImdsServiceBuilder;
    private HttpServer server;

    public Ec2ImdsHttpFixture(Ec2ImdsServiceBuilder ec2ImdsServiceBuilder) {
        this.ec2ImdsServiceBuilder = ec2ImdsServiceBuilder;
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    public void stop(int delay) {
        server.stop(delay);
    }

    protected void before() throws Throwable {
        server = HttpServer.create(resolveAddress(), 0);
        server.createContext("/", Objects.requireNonNull(ec2ImdsServiceBuilder.buildHandler()));
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

    @SuppressForbidden(reason = "deliberately adjusting system property for endpoint override for use in internal-cluster tests")
    public static Releasable withEc2MetadataServiceEndpointOverride(String endpointOverride) {
        final PrivilegedAction<String> resetProperty = System.getProperty(ENDPOINT_OVERRIDE_SYSPROP_NAME) instanceof String originalValue
            ? () -> System.setProperty(ENDPOINT_OVERRIDE_SYSPROP_NAME, originalValue)
            : () -> System.clearProperty(ENDPOINT_OVERRIDE_SYSPROP_NAME);
        doPrivileged(() -> System.setProperty(ENDPOINT_OVERRIDE_SYSPROP_NAME, endpointOverride));
        return () -> doPrivileged(resetProperty);
    }

    private static void doPrivileged(PrivilegedAction<?> privilegedAction) {
        AccessController.doPrivileged(privilegedAction);
    }

    public static void runWithFixture(Ec2ImdsServiceBuilder ec2ImdsServiceBuilder, CheckedConsumer<Ec2ImdsHttpFixture, Exception> action) {
        final var imdsFixture = new Ec2ImdsHttpFixture(ec2ImdsServiceBuilder);
        try {
            imdsFixture.apply(new Statement() {
                @Override
                public void evaluate() throws Exception {
                    action.accept(imdsFixture);
                }
            }, Description.EMPTY).evaluate();
        } catch (Throwable e) {
            throw new AssertionError(e);
        }
    }

}
