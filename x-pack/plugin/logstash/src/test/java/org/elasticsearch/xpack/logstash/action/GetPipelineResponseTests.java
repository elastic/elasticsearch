/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

public class GetPipelineResponseTests extends AbstractWireSerializingTestCase<GetPipelineResponse> {

    @Override
    protected Writeable.Reader<GetPipelineResponse> instanceReader() {
        return GetPipelineResponse::new;
    }

    @Override
    protected GetPipelineResponse createTestInstance() {
        final int numPipelines = randomIntBetween(1, 10);
        final Map<String, BytesReference> map = Maps.newMapWithExpectedSize(numPipelines);
        for (int i = 0; i < numPipelines; i++) {
            final String name = randomAlphaOfLengthBetween(2, 10);
            final BytesReference ref = new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 16)));
            map.put(name, ref);
        }
        return new GetPipelineResponse(map);
    }

    @Override
    protected GetPipelineResponse mutateInstance(GetPipelineResponse instance) {
        Map<String, BytesReference> map = Maps.newMapWithExpectedSize(instance.pipelines().size() + 1);
        map.putAll(instance.pipelines());
        map.put(randomAlphaOfLengthBetween(2, 10), new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 16))));
        return new GetPipelineResponse(map);
    }
}
