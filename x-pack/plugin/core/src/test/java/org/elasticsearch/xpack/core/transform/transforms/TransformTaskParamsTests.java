/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.utils.TransformConfigVersionUtils;

import java.io.IOException;
import java.time.Instant;

public class TransformTaskParamsTests extends AbstractSerializingTransformTestCase<TransformTaskParams> {

    private static final TransportVersion TRANSFORM_START_INITIAL_DELAY = TransportVersion.fromName("transform_start_initial_delay");

    private static TransformTaskParams randomTransformTaskParams() {
        return new TransformTaskParams(
            randomAlphaOfLengthBetween(1, 10),
            randomBoolean() ? TransformConfigVersionUtils.randomVersion() : null,
            randomBoolean() ? Instant.ofEpochMilli(randomLongBetween(0, 1_000_000_000_000L)) : null,
            randomBoolean() ? TimeValue.timeValueSeconds(randomLongBetween(1, 24 * 60 * 60)) : null,
            randomBoolean(),
            randomBoolean() ? TimeValue.timeValueSeconds(randomLongBetween(0, 24 * 60 * 60)) : null
        );
    }

    @Override
    protected TransformTaskParams doParseInstance(XContentParser parser) throws IOException {
        return TransformTaskParams.PARSER.apply(parser, null);
    }

    @Override
    protected TransformTaskParams createTestInstance() {
        return randomTransformTaskParams();
    }

    @Override
    protected TransformTaskParams mutateInstance(TransformTaskParams instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TransformTaskParams mutateInstanceForVersion(TransformTaskParams instance, TransportVersion version) {
        if (version.supports(TRANSFORM_START_INITIAL_DELAY)) {
            return instance;
        }
        // Older nodes do not know about initial_delay, so on read it falls back to null.
        return new TransformTaskParams(
            instance.getId(),
            instance.getVersion(),
            instance.from(),
            instance.getFrequency(),
            instance.requiresRemote()
        );
    }

    @Override
    protected Reader<TransformTaskParams> instanceReader() {
        return TransformTaskParams::new;
    }
}
