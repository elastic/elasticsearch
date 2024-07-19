/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.List;

public class GetInternalInferenceUsageActionResponseTests extends AbstractBWCWireSerializationTestCase<
    GetInternalInferenceUsageAction.Response> {
    public static GetInternalInferenceUsageAction.Response createRandom() {
        List<GetInternalInferenceUsageAction.NodeResponse> responses = randomList(
            2,
            10,
            GetInternalInferenceUsageActionNodeResponseTests::createRandom
        );

        return new GetInternalInferenceUsageAction.Response(ClusterName.DEFAULT, responses, List.of());
    }

    @Override
    protected Writeable.Reader<GetInternalInferenceUsageAction.Response> instanceReader() {
        return GetInternalInferenceUsageAction.Response::new;
    }

    @Override
    protected GetInternalInferenceUsageAction.Response createTestInstance() {
        return createRandom();
    }

    @Override
    protected GetInternalInferenceUsageAction.Response mutateInstance(GetInternalInferenceUsageAction.Response instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetInternalInferenceUsageAction.Response mutateInstanceForVersion(
        GetInternalInferenceUsageAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
