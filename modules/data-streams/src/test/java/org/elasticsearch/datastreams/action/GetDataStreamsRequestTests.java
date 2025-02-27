/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.datastreams.GetDataStreamAction.Request;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class GetDataStreamsRequestTests extends AbstractWireSerializingTestCase<GetDataStreamsRequestTests.RequestWrapper> {

    @Override
    protected Writeable.Reader<RequestWrapper> instanceReader() {
        return RequestWrapper::new;
    }

    @Override
    protected RequestWrapper createTestInstance() {
        var req = new Request(TEST_REQUEST_TIMEOUT, switch (randomIntBetween(1, 4)) {
            case 1 -> generateRandomStringArray(3, 8, false, false);
            case 2 -> {
                String[] parameters = generateRandomStringArray(3, 8, false, false);
                for (int k = 0; k < parameters.length; k++) {
                    parameters[k] = parameters[k] + "*";
                }
                yield parameters;
            }
            case 3 -> new String[] { "*" };
            default -> null;
        });
        req.verbose(randomBoolean());
        return new RequestWrapper(req);
    }

    @Override
    protected RequestWrapper mutateInstance(RequestWrapper requestWrapper) {
        var instance = requestWrapper.request();
        var indices = instance.indices();
        var indicesOpts = instance.indicesOptions();
        var includeDefaults = instance.includeDefaults();
        var verbose = instance.verbose();
        switch (randomIntBetween(0, 3)) {
            case 0 -> indices = randomValueOtherThan(indices, () -> generateRandomStringArray(3, 8, false, false));
            case 1 -> indicesOpts = randomValueOtherThan(
                indicesOpts,
                () -> IndicesOptions.fromOptions(
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean()
                )
            );
            case 2 -> includeDefaults = includeDefaults == false;
            case 3 -> verbose = verbose == false;
        }
        var newReq = new Request(instance.masterTimeout(), indices);
        newReq.includeDefaults(includeDefaults);
        newReq.indicesOptions(indicesOpts);
        newReq.verbose(verbose);
        return new RequestWrapper(newReq);
    }

    /**
     * We need to wrap the request class because the request itself doesn't need to be able to serialize to the wire because
     * a new node will never forward the request to a different node. Therefore, we moved the serialization here to still be able to test
     * the deserialization.
     */
    public static class RequestWrapper extends LocalClusterStateHelper.RequestSerializationWrapper<Request> {

        RequestWrapper(Request request) {
            super(request);
        }

        RequestWrapper(StreamInput in) throws IOException {
            this(new Request(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(request().getNames());
            request().indicesOptions().writeIndicesOptions(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                out.writeBoolean(request().includeDefaults());
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeBoolean(request().verbose());
            }
        }
    }
}
