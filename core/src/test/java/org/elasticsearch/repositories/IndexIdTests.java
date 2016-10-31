/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Tests for the {@link IndexId} class.
 */
public class IndexIdTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        // assert equals and hashcode
        String name = randomAsciiOfLength(8);
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
        //assert not equals when name or id differ
        indexId2 = new IndexId(randomAsciiOfLength(8), id);
        assertNotEquals(indexId1, indexId2);
        assertNotEquals(indexId1.hashCode(), indexId2.hashCode());
        indexId2 = new IndexId(name, UUIDs.randomBase64UUID());
        assertNotEquals(indexId1, indexId2);
        assertNotEquals(indexId1.hashCode(), indexId2.hashCode());
    }

    public void testSerialization() throws IOException {
        IndexId indexId = new IndexId(randomAsciiOfLength(8), UUIDs.randomBase64UUID());
        BytesStreamOutput out = new BytesStreamOutput();
        indexId.writeTo(out);
        assertEquals(indexId, new IndexId(out.bytes().streamInput()));
    }

    public void testXContent() throws IOException {
        IndexId indexId = new IndexId(randomAsciiOfLength(8), UUIDs.randomBase64UUID());
        XContentBuilder builder = JsonXContent.contentBuilder();
        indexId.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent().createParser(builder.bytes());
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
