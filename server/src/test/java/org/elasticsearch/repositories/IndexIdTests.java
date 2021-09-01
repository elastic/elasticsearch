/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Tests for the {@link IndexId} class.
 */
public class IndexIdTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        // assert equals and hashcode
        String name = randomAlphaOfLength(8);
        String id = UUIDs.randomBase64UUID();
        IndexId indexId1 = new IndexId(name, id);
        IndexId indexId2 = new IndexId(name, id);
        assertEquals(indexId1, indexId2);
        assertEquals(indexId1.hashCode(), indexId2.hashCode());
        // assert equals when using index name for id
        id = name;
        indexId1 = new IndexId(name, id);
        indexId2 = new IndexId(name, id);
        assertEquals(indexId1, indexId2);
        assertEquals(indexId1.hashCode(), indexId2.hashCode());
        // assert not equals when name or id differ
        indexId2 = new IndexId(randomAlphaOfLength(8), id);
        assertNotEquals(indexId1, indexId2);
        assertNotEquals(indexId1.hashCode(), indexId2.hashCode());
        indexId2 = new IndexId(name, UUIDs.randomBase64UUID());
        assertNotEquals(indexId1, indexId2);
        assertNotEquals(indexId1.hashCode(), indexId2.hashCode());
    }

    public void testSerialization() throws IOException {
        IndexId indexId = new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
        BytesStreamOutput out = new BytesStreamOutput();
        indexId.writeTo(out);
        assertEquals(indexId, new IndexId(out.bytes().streamInput()));
    }

    public void testXContent() throws IOException {
        IndexId indexId = new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
        XContentBuilder builder = JsonXContent.contentBuilder();
        indexId.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        String name = null;
        String id = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            final String currentFieldName = parser.currentName();
            parser.nextToken();
            if (currentFieldName.equals(IndexId.NAME)) {
                name = parser.text();
            } else if (currentFieldName.equals(IndexId.ID)) {
                id = parser.text();
            }
        }
        assertNotNull(name);
        assertNotNull(id);
        assertEquals(indexId, new IndexId(name, id));
    }
}
