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

import java.util.Date;

public class FlushAcknowledgementTests extends AbstractSerializingTestCase<FlushAcknowledgement> {

    @Override
    protected FlushAcknowledgement doParseInstance(XContentParser parser) {
        return FlushAcknowledgement.PARSER.apply(parser, null);
    }

    @Override
    protected FlushAcknowledgement createTestInstance() {
        return new FlushAcknowledgement(randomAlphaOfLengthBetween(1, 20), new Date(randomNonNegativeLong()));
    }

    @Override
    protected Reader<FlushAcknowledgement> instanceReader() {
        return FlushAcknowledgement::new;
    }

}
