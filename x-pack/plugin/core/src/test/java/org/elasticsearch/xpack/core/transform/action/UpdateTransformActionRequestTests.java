/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.compat.UpdateTransformActionPre78;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;

import java.io.IOException;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdateTests.randomTransformConfigUpdate;

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
        assertEquals(newRequest.getUpdate().getSource(), oldRequest.getUpdate().getSource());
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
        assertEquals(newRequest.getUpdate().getSource(), newRequestFromOld.getUpdate().getSource());
        assertEquals(newRequest.getUpdate().getSyncConfig(), newRequestFromOld.getUpdate().getSyncConfig());
        assertEquals(newRequest.isDeferValidation(), newRequestFromOld.isDeferValidation());
    }

}
