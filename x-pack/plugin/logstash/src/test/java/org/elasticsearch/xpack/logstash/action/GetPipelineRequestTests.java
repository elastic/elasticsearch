/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GetPipelineRequestTests extends AbstractWireSerializingTestCase<GetPipelineRequest> {

    @Override
    protected Writeable.Reader<GetPipelineRequest> instanceReader() {
        return GetPipelineRequest::new;
    }

    @Override
    protected GetPipelineRequest createTestInstance() {
        return new GetPipelineRequest(randomList(0, 50, () -> randomAlphaOfLengthBetween(2, 10)));
    }

    @Override
    protected GetPipelineRequest mutateInstance(GetPipelineRequest instance) {
        List<String> ids = new ArrayList<>(instance.ids());
        if (randomBoolean()) {
            // append another ID
            ids.add(randomAlphaOfLengthBetween(2, 10));
        } else {
            ids = ids.stream().map(id -> id + randomAlphaOfLength(1)).collect(Collectors.toList());
        }
        return new GetPipelineRequest(ids);
    }
}
