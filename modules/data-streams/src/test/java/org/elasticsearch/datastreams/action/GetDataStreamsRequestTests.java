/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.datastreams.GetDataStreamAction.Request;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetDataStreamsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
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
        return req;
    }

    @Override
    protected Request mutateInstance(Request instance) {
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
        var newReq = new Request(instance.masterNodeTimeout(), indices);
        newReq.includeDefaults(includeDefaults);
        newReq.indicesOptions(indicesOpts);
        newReq.verbose(verbose);
        return newReq;
    }

}
