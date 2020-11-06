/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction.Request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class UpdateFilterActionRequestTests extends AbstractSerializingTestCase<Request> {

    private String filterId = randomAlphaOfLength(20);

    @Override
    protected Request createTestInstance() {
        UpdateFilterAction.Request request = new UpdateFilterAction.Request(filterId);
        if (randomBoolean()) {
            request.setDescription(randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            request.setAddItems(generateRandomStrings());
        }
        if (randomBoolean()) {
            request.setRemoveItems(generateRandomStrings());
        }
        return request;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    private static Collection<String> generateRandomStrings() {
        int size = randomIntBetween(0, 10);
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            strings.add(randomAlphaOfLength(20));
        }
        return strings;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(filterId, parser);
    }
}
