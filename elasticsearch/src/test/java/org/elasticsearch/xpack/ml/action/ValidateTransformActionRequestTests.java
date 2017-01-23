/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.ValidateTransformAction.Request;
import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.config.transform.TransformType;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

public class ValidateTransformActionRequestTests extends AbstractStreamableXContentTestCase<ValidateTransformAction.Request> {

    @Override
    protected Request createTestInstance() {
        TransformType transformType = randomFrom(TransformType.values());
        TransformConfig transform = new TransformConfig(transformType.prettyName());
        return new Request(transform);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return Request.parseRequest(parser);
    }

}
