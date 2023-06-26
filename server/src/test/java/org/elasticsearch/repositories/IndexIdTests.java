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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

/**
 * Tests for the {@link IndexId} class.
 */
public class IndexIdTests extends AbstractWireSerializingTestCase<IndexId> {

    @Override
    protected Writeable.Reader<IndexId> instanceReader() {
        return IndexId::new;
    }

    @Override
    protected IndexId createTestInstance() {
        return new IndexId(randomIdentifier(), UUIDs.randomBase64UUID());
    }

    @Override
    protected IndexId mutateInstance(IndexId instance) {
        return switch (randomInt(1)) {
            case 0 -> new IndexId(randomValueOtherThan(instance.getName(), ESTestCase::randomIdentifier), instance.getId());
            case 1 -> new IndexId(instance.getName(), randomValueOtherThan(instance.getId(), UUIDs::randomBase64UUID));
            default -> throw new RuntimeException("unreachable");
        };
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
