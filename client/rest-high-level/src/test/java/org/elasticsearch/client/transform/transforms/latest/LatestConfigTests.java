/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.latest;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Predicate;

public class LatestConfigTests extends AbstractXContentTestCase<LatestConfig> {

    public static LatestConfig randomLatestConfig() {
        return new LatestConfig(
            new ArrayList<>(randomUnique(() -> randomAlphaOfLengthBetween(1, 10), randomIntBetween(1, 10))),
            randomAlphaOfLengthBetween(1, 10)
        );
    }

    @Override
    protected LatestConfig doParseInstance(XContentParser parser) throws IOException {
        return LatestConfig.fromXContent(parser);
    }

    @Override
    protected LatestConfig createTestInstance() {
        return randomLatestConfig();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }
}
