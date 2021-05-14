/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction.Response;

import java.io.IOException;
import java.util.Map;

public class PreviewTransformsActionResponseWireTests extends AbstractWireSerializingTransformTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return PreviewTransformsActionResponseTests.randomPreviewResponse();
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }

    public void testBackwardsSerialization76() throws IOException {
        Response response = PreviewTransformsActionResponseTests.randomPreviewResponse();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_6_0);
            output.writeInt(response.getDocs().size());
            for (Map<String, Object> doc : response.getDocs()) {
                output.writeMapWithConsistentOrder(doc);
            }

            output.writeMap(response.getGeneratedDestIndexSettings().getMappings());
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_6_0);
                Response streamedResponse = new Response(in);
                assertEquals(
                    response.getGeneratedDestIndexSettings().getMappings(),
                    streamedResponse.getGeneratedDestIndexSettings().getMappings()
                );
                assertEquals(response.getDocs(), streamedResponse.getDocs());
            }
        }
    }

}
