/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.job.config.MlFilterTests;

public class PutFilterActionResponseTests extends AbstractStreamableTestCase<PutFilterAction.Response> {

    @Override
    protected PutFilterAction.Response createTestInstance() {
        return new PutFilterAction.Response(MlFilterTests.createRandom());
    }

    @Override
    protected PutFilterAction.Response createBlankInstance() {
        return new PutFilterAction.Response();
    }
}
