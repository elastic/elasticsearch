/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.s3;

import org.jetbrains.annotations.NotNull;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class S3ExternalRule implements S3Fixture {
    @NotNull
    @Override
    public Statement apply(@NotNull Statement base, @NotNull Description description) {
        return base;
    }

    @Override
    public S3Fixture withExposedService(String serviceName) {
        return this;
    }

    @Override
    public String getServiceUrl(String serviceName) {
        return "http://localhost:80";
    }
}
