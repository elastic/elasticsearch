/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.inference.results.ModelStoreResponseTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;

public class StoreInferenceEndpointsActionResponseTests extends AbstractBWCWireSerializationTestCase<
    StoreInferenceEndpointsAction.Response> {

    @Override
    protected StoreInferenceEndpointsAction.Response mutateInstanceForVersion(
        StoreInferenceEndpointsAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<StoreInferenceEndpointsAction.Response> instanceReader() {
        return StoreInferenceEndpointsAction.Response::new;
    }

    @Override
    protected StoreInferenceEndpointsAction.Response createTestInstance() {
        return new StoreInferenceEndpointsAction.Response(randomList(5, ModelStoreResponseTests::randomModelStoreResponse));
    }

    @Override
    protected StoreInferenceEndpointsAction.Response mutateInstance(StoreInferenceEndpointsAction.Response instance) throws IOException {
        var newResults = new ArrayList<>(instance.getResults());
        newResults.add(ModelStoreResponseTests.randomModelStoreResponse());
        return new StoreInferenceEndpointsAction.Response(newResults);
    }
}
