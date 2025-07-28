/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class SystemIndexMigrationTaskParamsXContentTests extends AbstractXContentTestCase<SystemIndexMigrationTaskParams> {

    @Override
    protected SystemIndexMigrationTaskParams createTestInstance() {
        return new SystemIndexMigrationTaskParams();
    }

    @Override
    protected SystemIndexMigrationTaskParams doParseInstance(XContentParser parser) throws IOException {
        return SystemIndexMigrationTaskParams.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(SystemIndexMigrationExecutor.getNamedXContentParsers());
    }
}
