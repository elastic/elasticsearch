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
import java.util.function.Predicate;

public class SystemIndexMigrationTaskStateXContentTests extends AbstractXContentTestCase<SystemIndexMigrationTaskState> {

    @Override
    protected SystemIndexMigrationTaskState createTestInstance() {
        return SystemIndexMigrationTaskStateTests.randomSystemIndexMigrationTask();
    }

    @Override
    protected SystemIndexMigrationTaskState doParseInstance(XContentParser parser) throws IOException {
        return SystemIndexMigrationTaskState.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // featureCallbackMetadata is a Map<String,Object> so adding random fields there make no sense
        return p -> p.startsWith(SystemIndexMigrationTaskState.FEATURE_METADATA_MAP_FIELD.getPreferredName());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(SystemIndexMigrationExecutor.getNamedXContentParsers());
    }
}
