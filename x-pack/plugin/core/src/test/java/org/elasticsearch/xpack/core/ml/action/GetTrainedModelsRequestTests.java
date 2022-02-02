/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Includes;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Request;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetTrainedModelsRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(
            randomAlphaOfLength(20),
            randomBoolean() ? null : randomList(10, () -> randomAlphaOfLength(10)),
            randomBoolean()
                ? null
                : Stream.generate(
                    () -> randomFrom(
                        Includes.DEFINITION,
                        Includes.TOTAL_FEATURE_IMPORTANCE,
                        Includes.FEATURE_IMPORTANCE_BASELINE,
                        Includes.HYPERPARAMETERS
                    )
                ).limit(4).collect(Collectors.toSet())
        );
        request.setPageParams(new PageParams(randomIntBetween(0, 100), randomIntBetween(0, 100)));
        return request;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, Version version) {
        return instance;
    }
}
