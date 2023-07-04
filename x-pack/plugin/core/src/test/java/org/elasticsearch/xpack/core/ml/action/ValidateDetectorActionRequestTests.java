/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.Detector;

public class ValidateDetectorActionRequestTests extends AbstractXContentSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Detector.Builder detector;
        if (randomBoolean()) {
            detector = new Detector.Builder(randomFrom(Detector.COUNT_WITHOUT_FIELD_FUNCTIONS), null);
        } else {
            detector = new Detector.Builder(randomFrom(Detector.FIELD_NAME_FUNCTIONS), randomAlphaOfLengthBetween(1, 20));
        }
        return new Request(detector.build());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(parser);
    }

}
