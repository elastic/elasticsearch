/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.alias.RandomAliasActionsGenerator.randomAliasAction;
import static org.hamcrest.CoreMatchers.equalTo;

public class IndicesAliasesRequestTests extends ESTestCase {

    public void testToAndFromXContent() throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = createTestInstance();
        XContentType xContentType = randomFrom(XContentType.values());

        BytesReference shuffled = toShuffledXContent(indicesAliasesRequest, xContentType, ToXContent.EMPTY_PARAMS, true, "filter");

        IndicesAliasesRequest parsedIndicesAliasesRequest;
        try (XContentParser parser = createParser(xContentType.xContent(), shuffled)) {
            parsedIndicesAliasesRequest = IndicesAliasesRequest.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        for (int i = 0; i < parsedIndicesAliasesRequest.getAliasActions().size(); i++) {
            AliasActions expectedAction = indicesAliasesRequest.getAliasActions().get(i);
            AliasActions actualAction = parsedIndicesAliasesRequest.getAliasActions().get(i);
            assertThat(actualAction, equalTo(expectedAction));
        }
    }

    private IndicesAliasesRequest createTestInstance() {
        int numItems = randomIntBetween(0, 32);
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        if (randomBoolean()) {
            request.timeout(randomTimeValue());
        }

        if (randomBoolean()) {
            request.masterNodeTimeout(randomTimeValue());
        }
        for (int i = 0; i < numItems; i++) {
            request.addAliasAction(randomAliasAction());
        }
        return request;
    }
}
