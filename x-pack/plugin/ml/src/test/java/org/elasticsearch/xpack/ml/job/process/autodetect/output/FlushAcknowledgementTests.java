/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;

import java.time.Instant;

public class FlushAcknowledgementTests extends AbstractXContentSerializingTestCase<FlushAcknowledgement> {

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
    protected FlushAcknowledgement mutateInstance(FlushAcknowledgement instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<FlushAcknowledgement> instanceReader() {
        return FlushAcknowledgement::new;
    }

}
