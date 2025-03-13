/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;

import java.io.IOException;
import java.time.Instant;

public class TransformTests extends AbstractSerializingTransformTestCase<TransformTaskParams> {

    @Override
    protected TransformTaskParams doParseInstance(XContentParser parser) throws IOException {
        return TransformTaskParams.PARSER.apply(parser, null);
    }

    @Override
    protected TransformTaskParams createTestInstance() {
        return new TransformTaskParams(
            randomAlphaOfLength(10),
            randomBoolean() ? null : TransformConfigVersion.CURRENT,
            randomBoolean() ? Instant.ofEpochMilli(randomLongBetween(0, 1_000_000_000_000L)) : null,
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean()
        );
    }

    @Override
    protected TransformTaskParams mutateInstance(TransformTaskParams instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<TransformTaskParams> instanceReader() {
        return TransformTaskParams::new;
    }
}
