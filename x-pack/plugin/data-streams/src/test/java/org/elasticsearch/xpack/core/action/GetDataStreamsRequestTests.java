/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.GetDataStreamAction.Request;

public class GetDataStreamsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(switch (randomIntBetween(1, 4)) {
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
    }

}
