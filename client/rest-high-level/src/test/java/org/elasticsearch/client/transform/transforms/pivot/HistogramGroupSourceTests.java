/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class HistogramGroupSourceTests extends AbstractXContentTestCase<HistogramGroupSource> {

    public static HistogramGroupSource randomHistogramGroupSource() {
        String field = randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : null;
        Script script = randomBoolean() ? new Script(randomAlphaOfLengthBetween(1, 10)) : null;
        boolean missingBucket = randomBoolean();
        double interval = randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false);
        return new HistogramGroupSource(field, script, missingBucket, interval);
    }

    @Override
    protected HistogramGroupSource doParseInstance(XContentParser parser) throws IOException {
        return HistogramGroupSource.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected HistogramGroupSource createTestInstance() {
        return randomHistogramGroupSource();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }
}
