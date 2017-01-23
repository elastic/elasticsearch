/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.PutListAction.Request;
import org.elasticsearch.xpack.ml.job.config.ListDocument;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

import java.util.ArrayList;
import java.util.List;

public class CreateListActionRequestTests extends AbstractStreamableXContentTestCase<PutListAction.Request> {

    @Override
    protected Request createTestInstance() {
        int size = randomInt(10);
        List<String> items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(randomAsciiOfLengthBetween(1, 20));
        }
        ListDocument listDocument = new ListDocument(randomAsciiOfLengthBetween(1, 20), items);
        return new PutListAction.Request(listDocument);
    }

    @Override
    protected Request createBlankInstance() {
        return new PutListAction.Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return PutListAction.Request.parseRequest(parser);
    }

}
