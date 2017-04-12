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

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AliasActionsTests extends ESTestCase {
    public void testValidate() {
        AliasActions.Type type = randomFrom(AliasActions.Type.values());
        if (type == AliasActions.Type.REMOVE_INDEX) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> new AliasActions(type).validate());
            assertEquals("One of [index] or [indices] is required", e.getMessage());
        } else {
            Exception e = expectThrows(IllegalArgumentException.class,
                    () -> new AliasActions(type).alias(randomAlphaOfLength(5)).validate());
            assertEquals("One of [index] or [indices] is required", e.getMessage());
            e = expectThrows(IllegalArgumentException.class, () -> new AliasActions(type).index(randomAlphaOfLength(5)).validate());
            assertEquals("One of [alias] or [aliases] is required", e.getMessage());
        }
    }

    public void testEmptyIndex() {
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> new AliasActions(randomFrom(AliasActions.Type.values())).index(null));
        assertEquals("[index] can't be empty string", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> new AliasActions(randomFrom(AliasActions.Type.values())).index(""));
        assertEquals("[index] can't be empty string", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> new AliasActions(randomFrom(AliasActions.Type.values())).indices((String[]) null));
        assertEquals("[indices] can't be empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> new AliasActions(randomFrom(AliasActions.Type.values())).indices(new String[0]));
        assertEquals("[indices] can't be empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> new AliasActions(randomFrom(AliasActions.Type.values())).indices("test", null));
        assertEquals("[indices] can't contain empty string", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> new AliasActions(randomFrom(AliasActions.Type.values())).indices("test", ""));
        assertEquals("[indices] can't contain empty string", e.getMessage());
    }

    public void testEmptyAlias() {
        AliasActions.Type type = randomValueOtherThan(AliasActions.Type.REMOVE_INDEX, () -> randomFrom(AliasActions.Type.values()));
        Exception e = expectThrows(IllegalArgumentException.class, () -> new AliasActions(type).alias(null));
        assertEquals("[alias] can't be empty string", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new AliasActions(type).alias(""));
        assertEquals("[alias] can't be empty string", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new AliasActions(type).aliases((String[]) null));
        assertEquals("[aliases] can't be empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new AliasActions(type).aliases(new String[0]));
        assertEquals("[aliases] can't be empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new AliasActions(type).aliases("test", null));
        assertEquals("[aliases] can't contain empty string", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new AliasActions(type).aliases("test", ""));
        assertEquals("[aliases] can't contain empty string", e.getMessage());
    }

    public void testBadOptionsInNonIndex() {
        AliasActions action = randomBoolean() ? AliasActions.remove() : AliasActions.removeIndex();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.routing("test"));
        assertEquals("[routing] is unsupported for [" + action.actionType() + "]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> action.searchRouting("test"));
        assertEquals("[search_routing] is unsupported for [" + action.actionType() + "]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> action.indexRouting("test"));
        assertEquals("[index_routing] is unsupported for [" + action.actionType() + "]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> action.filter("test"));
        assertEquals("[filter] is unsupported for [" + action.actionType() + "]", e.getMessage());
    }

    public void testParseAdd() throws IOException {
        String[] indices = generateRandomStringArray(10, 5, false, false);
        String[] aliases = generateRandomStringArray(10, 5, false, false);
        Map<String, Object> filter = randomBoolean() ? randomMap(5) : null;
        Object searchRouting = randomBoolean() ? randomRouting() : null;
        Object indexRouting = randomBoolean() ? randomBoolean() ? searchRouting : randomRouting() : null;
        XContentBuilder b = XContentBuilder.builder(randomFrom(XContentType.values()).xContent());
        b.startObject(); {
            b.startObject("add"); {
                if (indices.length > 1 || randomBoolean()) {
                    b.array("indices", indices);
                } else {
                    b.field("index", indices[0]);
                }
                if (aliases.length > 1 || randomBoolean()) {
                    b.array("aliases", aliases);
                } else {
                    b.field("alias", aliases[0]);
                }
                if (filter != null) {
                    b.field("filter", filter);
                }
                if (searchRouting != null) {
                    if (searchRouting.equals(indexRouting)) {
                        b.field("routing", searchRouting);
                    } else {
                        b.field("search_routing", searchRouting);
                    }
                }
                if (indexRouting != null && false == indexRouting.equals(searchRouting)) {
                    b.field("index_routing", indexRouting);
                }
            }
            b.endObject();
        }
        b.endObject();
        b = shuffleXContent(b, "filter");
        try (XContentParser parser = createParser(b)) {
            AliasActions action = AliasActions.PARSER.apply(parser, null);
            assertEquals(AliasActions.Type.ADD, action.actionType());
            assertThat(action.indices(), equalTo(indices));
            assertThat(action.aliases(), equalTo(aliases));
            if (filter == null || filter.isEmpty()) {
                assertNull(action.filter());
            } else {
                assertEquals(XContentFactory.contentBuilder(XContentType.JSON).map(filter).string(), action.filter());
            }
            assertEquals(Objects.toString(searchRouting, null), action.searchRouting());
            assertEquals(Objects.toString(indexRouting, null), action.indexRouting());
        }
    }

    public void testParseAddDefaultRouting() throws IOException {
        String index = randomAlphaOfLength(5);
        String alias = randomAlphaOfLength(5);
        Object searchRouting = randomRouting();
        Object indexRouting = randomRouting();
        XContentBuilder b = XContentBuilder.builder(randomFrom(XContentType.values()).xContent());
        b.startObject(); {
            b.startObject("add"); {
                b.field("index", index);
                b.field("alias", alias);
                if (randomBoolean()) {
                    b.field("routing", searchRouting);
                    b.field("index_routing", indexRouting);
                } else {
                    b.field("search_routing", searchRouting);
                    b.field("routing", indexRouting);
                }
            }
            b.endObject();
        }
        b.endObject();
        b = shuffleXContent(b);
        try (XContentParser parser = createParser(b)) {
            AliasActions action = AliasActions.PARSER.apply(parser, null);
            assertEquals(AliasActions.Type.ADD, action.actionType());
            assertThat(action.indices(), arrayContaining(index));
            assertThat(action.aliases(), arrayContaining(alias));
            assertEquals(searchRouting.toString(), action.searchRouting());
            assertEquals(indexRouting.toString(), action.indexRouting());
        }
    }

    public void testParseRemove() throws IOException {
        String[] indices = generateRandomStringArray(10, 5, false, false);
        String[] aliases = generateRandomStringArray(10, 5, false, false);
        XContentBuilder b = XContentBuilder.builder(randomFrom(XContentType.values()).xContent());
        b.startObject(); {
            b.startObject("remove"); {
                if (indices.length > 1 || randomBoolean()) {
                    b.array("indices", indices);
                } else {
                    b.field("index", indices[0]);
                }
                if (aliases.length > 1 || randomBoolean()) {
                    b.array("aliases", aliases);
                } else {
                    b.field("alias", aliases[0]);
                }
            }
            b.endObject();
        }
        b.endObject();
        b = shuffleXContent(b);
        try (XContentParser parser = createParser(b)) {
            AliasActions action = AliasActions.PARSER.apply(parser, null);
            assertEquals(AliasActions.Type.REMOVE, action.actionType());
            assertThat(action.indices(), equalTo(indices));
            assertThat(action.aliases(), equalTo(aliases));
        }
    }

    public void testParseRemoveIndex() throws IOException {
        String[] indices = randomBoolean() ? new String[] {randomAlphaOfLength(5)} : generateRandomStringArray(10, 5, false, false);
        XContentBuilder b = XContentBuilder.builder(randomFrom(XContentType.values()).xContent());
        b.startObject(); {
            b.startObject("remove_index"); {
                if (indices.length > 1 || randomBoolean()) {
                    b.array("indices", indices);
                } else {
                    b.field("index", indices[0]);
                }
            }
            b.endObject();
        }
        b.endObject();
        b = shuffleXContent(b);
        try (XContentParser parser = createParser(b)) {
            AliasActions action = AliasActions.PARSER.apply(parser, null);
            assertEquals(AliasActions.Type.REMOVE_INDEX, action.actionType());
            assertArrayEquals(indices, action.indices());
            assertThat(action.aliases(), arrayWithSize(0));
        }
    }

    public void testParseIndexAndIndicesThrowsError() throws IOException {
        XContentBuilder b = XContentBuilder.builder(randomFrom(XContentType.values()).xContent());
        b.startObject(); {
            b.startObject(randomFrom("add", "remove")); {
                b.field("index", randomAlphaOfLength(5));
                b.array("indices", generateRandomStringArray(10, 5, false, false));
                b.field("alias", randomAlphaOfLength(5));
            }
            b.endObject();
        }
        b.endObject();
        try (XContentParser parser = createParser(b)) {
            Exception e = expectThrows(ParsingException.class, () -> AliasActions.PARSER.apply(parser, null));
            assertThat(e.getCause().getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("Only one of [index] and [indices] is supported", e.getCause().getCause().getMessage());
        }
    }

    public void testParseAliasAndAliasesThrowsError() throws IOException {
        XContentBuilder b = XContentBuilder.builder(randomFrom(XContentType.values()).xContent());
        b.startObject(); {
            b.startObject(randomFrom("add", "remove")); {
                b.field("index", randomAlphaOfLength(5));
                b.field("alias", randomAlphaOfLength(5));
                b.array("aliases", generateRandomStringArray(10, 5, false, false));
            }
            b.endObject();
        }
        b.endObject();
        try (XContentParser parser = createParser(b)) {
            Exception e = expectThrows(ParsingException.class, () -> AliasActions.PARSER.apply(parser, null));
            assertThat(e.getCause().getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("Only one of [alias] and [aliases] is supported", e.getCause().getCause().getMessage());
        }
    }

    public void testRoundTrip() throws IOException {
        AliasActions action = new AliasActions(randomFrom(AliasActions.Type.values()));
        if (randomBoolean()) {
            action.index(randomAlphaOfLength(5));
        } else {
            action.indices(generateRandomStringArray(5, 5, false, false));
        }
        if (action.actionType() != AliasActions.Type.REMOVE_INDEX) {
            if (randomBoolean()) {
                action.alias(randomAlphaOfLength(5));
            } else {
                action.aliases(generateRandomStringArray(5, 5, false, false));
            }
        }
        if (action.actionType() == AliasActions.Type.ADD) {
            if (randomBoolean()) {
                action.filter(randomAlphaOfLength(10));
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    action.routing(randomAlphaOfLength(5));
                } else {
                    action.searchRouting(randomAlphaOfLength(5));
                    action.indexRouting(randomAlphaOfLength(5));
                }
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            action.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                AliasActions read = new AliasActions(in);
                assertEquals(action, read);
            }
        }
    }


    private Map<String, Object> randomMap(int maxDepth) {
        int members = between(0, 5);
        Map<String, Object> result = new HashMap<>(members);
        for (int i = 0; i < members; i++) {
            Object value;
            switch (between(0, 3)) {
            case 0:
                if (maxDepth > 0) {
                    value = randomMap(maxDepth - 1);
                } else {
                    value = randomAlphaOfLength(5);
                }
                break;
            case 1:
                value = randomAlphaOfLength(5);
                break;
            case 2:
                value = randomBoolean();
                break;
            case 3:
                value = randomLong();
                break;
            default:
                throw new UnsupportedOperationException();
            }
            result.put(randomAlphaOfLength(5), value);
        }
        return result;
    }

    private Object randomRouting() {
        return randomBoolean() ? randomAlphaOfLength(5) : randomInt();
    }
}
