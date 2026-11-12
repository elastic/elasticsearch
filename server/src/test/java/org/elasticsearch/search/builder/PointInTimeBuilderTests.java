/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PointInTimeBuilderTests extends AbstractXContentSerializingTestCase<PointInTimeBuilder> {
    @Override
    protected PointInTimeBuilder doParseInstance(XContentParser parser) throws IOException {
        return PointInTimeBuilder.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<PointInTimeBuilder> instanceReader() {
        return PointInTimeBuilder::new;
    }

    @Override
    protected PointInTimeBuilder createTestInstance() {
        final PointInTimeBuilder pointInTime = new PointInTimeBuilder(new BytesArray(randomAlphaOfLength(20)));
        if (randomBoolean()) {
            pointInTime.setKeepAlive(randomTimeValue());
        }
        return pointInTime;
    }

    @Override
    protected PointInTimeBuilder mutateInstance(PointInTimeBuilder instance) {
        BytesReference encodedId = instance.getEncodedId();
        TimeValue keepAlive = instance.getKeepAlive();
        switch(between(0 , 1)) {
            case 0 -> encodedId = randomValueOtherThan(encodedId, () -> new BytesArray(randomAlphaOfLength(20)));
            case 1 -> keepAlive = randomValueOtherThan(keepAlive, () -> randomBoolean() ? null : randomTimeValue());
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PointInTimeBuilder(encodedId).setKeepAlive(keepAlive);
    }
}
