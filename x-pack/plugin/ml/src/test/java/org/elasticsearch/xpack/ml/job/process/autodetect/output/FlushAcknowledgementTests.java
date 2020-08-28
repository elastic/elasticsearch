/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;

import java.time.Instant;

public class FlushAcknowledgementTests extends AbstractSerializingTestCase<FlushAcknowledgement> {

    @Override
    protected FlushAcknowledgement doParseInstance(XContentParser parser) {
        return FlushAcknowledgement.PARSER.apply(parser, null);
    }

    @Override
    protected FlushAcknowledgement createTestInstance() {
        if (randomBoolean()) {
            return new FlushAcknowledgement(randomAlphaOfLengthBetween(1, 20), randomFrom(randomNonNegativeLong(), 0L, null));
        } else {
            return new FlushAcknowledgement(randomAlphaOfLengthBetween(1, 20), randomFrom(randomInstant(), Instant.EPOCH, null));
        }
    }

    @Override
    protected Reader<FlushAcknowledgement> instanceReader() {
        return FlushAcknowledgement::new;
    }

}
