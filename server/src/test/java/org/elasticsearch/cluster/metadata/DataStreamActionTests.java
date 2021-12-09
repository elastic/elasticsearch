/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DataStreamActionTests extends ESTestCase {

    public void testToAndFromXContent() throws IOException {
        DataStreamAction action = createTestInstance();
        XContentType xContentType = randomFrom(XContentType.values());

        BytesReference shuffled = toShuffledXContent(action, xContentType, ToXContent.EMPTY_PARAMS, true);

        DataStreamAction parsedAction;
        try (XContentParser parser = createParser(xContentType.xContent(), shuffled)) {
            parsedAction = DataStreamAction.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertThat(parsedAction.getType(), equalTo(action.getType()));
        assertThat(parsedAction.getDataStream(), equalTo(action.getDataStream()));
        assertThat(parsedAction.getIndex(), equalTo(action.getIndex()));
    }

    public static DataStreamAction createTestInstance() {
        DataStreamAction action = new DataStreamAction(
            randomBoolean() ? DataStreamAction.Type.ADD_BACKING_INDEX : DataStreamAction.Type.REMOVE_BACKING_INDEX
        );
        action.setDataStream(randomAlphaOfLength(8));
        action.setIndex(randomAlphaOfLength(8));
        return action;
    }

}
