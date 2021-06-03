/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.example.resthandler;

import org.elasticsearch.test.fixture.AbstractHttpFixture;

import java.io.IOException;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ExampleFixture extends AbstractHttpFixture {

    private final String message;

    private ExampleFixture(final String workingDir, final String message) {
        super(workingDir);
        this.message = Objects.requireNonNull(message);
    }

    @Override
    protected Response handle(final Request request) throws IOException {
        if ("GET".equals(request.getMethod()) && "/".equals(request.getPath())) {
            return new Response(200, TEXT_PLAIN_CONTENT_TYPE, message.getBytes(UTF_8));
        }
        return null;
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("ExampleFixture <working directory> <echo message>");
        }

        final ExampleFixture fixture = new ExampleFixture(args[0], args[1]);
        fixture.listen();
    }
}
