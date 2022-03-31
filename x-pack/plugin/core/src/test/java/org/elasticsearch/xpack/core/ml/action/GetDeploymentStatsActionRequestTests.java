/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetDeploymentStatsActionRequestTests extends AbstractWireSerializingTestCase<GetDeploymentStatsAction.Request> {
    @Override
    protected Writeable.Reader<GetDeploymentStatsAction.Request> instanceReader() {
        return GetDeploymentStatsAction.Request::new;
    }

    @Override
    protected GetDeploymentStatsAction.Request createTestInstance() {
        return new GetDeploymentStatsAction.Request(randomAlphaOfLength(5));
    }
}
