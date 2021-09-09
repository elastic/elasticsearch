/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class IndexTests extends ESTestCase {
    public void testToString() {
        assertEquals("[name/uuid]", new Index("name", "uuid").toString());
        assertEquals("[name]", new Index("name", ClusterState.UNKNOWN_UUID).toString());

        Index random = new Index(randomSimpleString(random(), 1, 100),
                usually() ? UUIDs.randomBase64UUID(random()) : ClusterState.UNKNOWN_UUID);
        assertThat(random.toString(), containsString(random.getName()));
        if (ClusterState.UNKNOWN_UUID.equals(random.getUUID())) {
            assertThat(random.toString(), not(containsString(random.getUUID())));
        } else {
            assertThat(random.toString(), containsString(random.getUUID()));
        }
    }

    public void testXContent() throws IOException {
        final String name = randomAlphaOfLengthBetween(4, 15);
        final String uuid = UUIDs.randomBase64UUID();
        final Index original = new Index(name, uuid);
        final XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            parser.nextToken(); // the beginning of the parser
            assertThat(Index.fromXContent(parser), equalTo(original));
        }
    }

    public void testEquals() {
        Index index1 = new Index("a", "a");
        Index index2 = new Index("a", "a");
        Index index3 = new Index("a", "b");
        Index index4 = new Index("b", "a");
        String s = "Some random other object";
        assertEquals(index1, index1);
        assertEquals(index1, index2);
        assertNotEquals(index1, null);
        assertNotEquals(index1, s);
        assertNotEquals(index1, index3);
        assertNotEquals(index1, index4);
    }
}
