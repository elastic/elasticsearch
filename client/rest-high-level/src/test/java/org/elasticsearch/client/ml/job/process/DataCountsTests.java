/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

public class DataCountsTests extends AbstractXContentTestCase<DataCounts> {

    private static Date randomDate() {
        return Date.from(ZonedDateTime.now(randomZone()).toInstant());
    }

    public static DataCounts createTestInstance(String jobId) {
        return new DataCounts(jobId, randomIntBetween(1, 1_000_000),
                randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                randomDate(), randomDate(), randomDate(), randomDate(), randomDate(),
                randomBoolean() ? null : Instant.now());
    }

    @Override
    public DataCounts createTestInstance() {
        return createTestInstance(randomAlphaOfLength(10));
    }

    @Override
    protected DataCounts doParseInstance(XContentParser parser) {
        return DataCounts.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

}
