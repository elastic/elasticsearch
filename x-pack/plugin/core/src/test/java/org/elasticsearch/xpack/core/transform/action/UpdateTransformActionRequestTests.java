/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.compat.UpdateTransformActionPre78;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;

import java.io.IOException;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdateTests.randomTransformConfigUpdate;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class UpdateTransformActionRequestTests extends AbstractWireSerializingTransformTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::fromStreamWithBWC;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomTransformConfigUpdate(), randomAlphaOfLength(10), randomBoolean());
        if (randomBoolean()) {
            request.setConfig(TransformConfigTests.randomTransformConfig());
        }
        return request;
    }

    public void testBWCPre78() throws IOException {
        Request newRequest = createTestInstance();
        UpdateTransformActionPre78.Request oldRequest = writeAndReadBWCObject(
            newRequest,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            UpdateTransformActionPre78.Request::new,
            Version.V_7_7_0
        );

        assertEquals(newRequest.getId(), oldRequest.getId());
        assertEquals(newRequest.getUpdate().getDestination(), oldRequest.getUpdate().getDestination());
        assertEquals(newRequest.getUpdate().getFrequency(), oldRequest.getUpdate().getFrequency());

        if (newRequest.getUpdate().getSource() != null) {
            assertThat(oldRequest.getUpdate().getSource().getIndex(), is(equalTo(newRequest.getUpdate().getSource().getIndex())));
            assertThat(
                oldRequest.getUpdate().getSource().getQueryConfig(),
                is(equalTo(newRequest.getUpdate().getSource().getQueryConfig())));
            // runtime_mappings was added in 7.12 so it is always empty after deserializing from 7.7
            assertThat(oldRequest.getUpdate().getSource().getRuntimeMappings(), is(anEmptyMap()));
        }
        assertEquals(newRequest.getUpdate().getSyncConfig(), oldRequest.getUpdate().getSyncConfig());
        assertEquals(newRequest.isDeferValidation(), oldRequest.isDeferValidation());

        Request newRequestFromOld = writeAndReadBWCObject(
            oldRequest,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            Request::fromStreamWithBWC,
            Version.V_7_7_0
        );

        assertEquals(newRequest.getId(), newRequestFromOld.getId());
        assertEquals(newRequest.getUpdate().getDestination(), newRequestFromOld.getUpdate().getDestination());
        assertEquals(newRequest.getUpdate().getFrequency(), newRequestFromOld.getUpdate().getFrequency());
        if (newRequest.getUpdate().getSource() != null) {
            assertThat(newRequestFromOld.getUpdate().getSource().getIndex(), is(equalTo(newRequest.getUpdate().getSource().getIndex())));
            assertThat(
                newRequestFromOld.getUpdate().getSource().getQueryConfig(),
                is(equalTo(newRequest.getUpdate().getSource().getQueryConfig())));
            // runtime_mappings was added in 7.12 so it is always empty after deserializing from 7.7
            assertThat(newRequestFromOld.getUpdate().getSource().getRuntimeMappings(), is(anEmptyMap()));
        }
        assertEquals(newRequest.getUpdate().getSyncConfig(), newRequestFromOld.getUpdate().getSyncConfig());
        assertEquals(newRequest.isDeferValidation(), newRequestFromOld.isDeferValidation());
    }

}
