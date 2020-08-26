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

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesTests extends AbstractSerializingTestCase<FieldCapabilities> {
    private static final String FIELD_NAME = "field";

    @Override
    protected FieldCapabilities doParseInstance(XContentParser parser) throws IOException {
        return FieldCapabilities.fromXContent(FIELD_NAME, parser);
    }

    @Override
    protected FieldCapabilities createTestInstance() {
        return randomFieldCaps(FIELD_NAME);
    }

    @Override
    protected Writeable.Reader<FieldCapabilities> instanceReader() {
        return FieldCapabilities::new;
    }

    public void testBuilder() {
        FieldCapabilities.Builder builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", true, false, Collections.emptyMap());
        builder.add("index2", true, false, Collections.emptyMap());
        builder.add("index3", true, false, Collections.emptyMap());

        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(true));
            assertThat(cap1.isAggregatable(), equalTo(false));
            assertNull(cap1.indices());
            assertNull(cap1.nonSearchableIndices());
            assertNull(cap1.nonAggregatableIndices());
            assertEquals(Collections.emptyMap(), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(true));
            assertThat(cap2.isAggregatable(), equalTo(false));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[]{"index1", "index2", "index3"}));
            assertNull(cap2.nonSearchableIndices());
            assertNull(cap2.nonAggregatableIndices());
            assertEquals(Collections.emptyMap(), cap2.meta());
        }

        builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, true, Collections.emptyMap());
        builder.add("index2", true, false, Collections.emptyMap());
        builder.add("index3", false, false, Collections.emptyMap());
        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(false));
            assertThat(cap1.isAggregatable(), equalTo(false));
            assertNull(cap1.indices());
            assertThat(cap1.nonSearchableIndices(), equalTo(new String[]{"index1", "index3"}));
            assertThat(cap1.nonAggregatableIndices(), equalTo(new String[]{"index2", "index3"}));
            assertEquals(Collections.emptyMap(), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(false));
            assertThat(cap2.isAggregatable(), equalTo(false));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[]{"index1", "index2", "index3"}));
            assertThat(cap2.nonSearchableIndices(), equalTo(new String[]{"index1", "index3"}));
            assertThat(cap2.nonAggregatableIndices(), equalTo(new String[]{"index2", "index3"}));
            assertEquals(Collections.emptyMap(), cap2.meta());
        }

        builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", true, true, Collections.emptyMap());
        builder.add("index2", true, true, Map.of("foo", "bar"));
        builder.add("index3", true, true, Map.of("foo", "quux"));
        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(true));
            assertThat(cap1.isAggregatable(), equalTo(true));
            assertNull(cap1.indices());
            assertNull(cap1.nonSearchableIndices());
            assertNull(cap1.nonAggregatableIndices());
            assertEquals(Map.of("foo", Set.of("bar", "quux")), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(true));
            assertThat(cap2.isAggregatable(), equalTo(true));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[]{"index1", "index2", "index3"}));
            assertNull(cap2.nonSearchableIndices());
            assertNull(cap2.nonAggregatableIndices());
            assertEquals(Map.of("foo", Set.of("bar", "quux")), cap2.meta());
        }
    }

    static FieldCapabilities randomFieldCaps(String fieldName) {
        String[] indices = null;
        if (randomBoolean()) {
            indices = new String[randomIntBetween(1, 5)];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        String[] nonSearchableIndices = null;
        if (randomBoolean()) {
            nonSearchableIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonSearchableIndices.length; i++) {
                nonSearchableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        String[] nonAggregatableIndices = null;
        if (randomBoolean()) {
            nonAggregatableIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonAggregatableIndices.length; i++) {
                nonAggregatableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }

        Map<String, Set<String>> meta;
        switch (randomInt(2)) {
        case 0:
            meta = Collections.emptyMap();
            break;
        case 1:
            meta = Map.of("foo", Set.of("bar"));
            break;
        default:
            meta = Map.of("foo", Set.of("bar", "baz"));
            break;
        }

        return new FieldCapabilities(fieldName,
            randomAlphaOfLengthBetween(5, 20), randomBoolean(), randomBoolean(),
            indices, nonSearchableIndices, nonAggregatableIndices, meta);
    }

    @Override
    protected FieldCapabilities mutateInstance(FieldCapabilities instance) {
        String name = instance.getName();
        String type = instance.getType();
        boolean isSearchable = instance.isSearchable();
        boolean isAggregatable = instance.isAggregatable();
        String[] indices = instance.indices();
        String[] nonSearchableIndices = instance.nonSearchableIndices();
        String[] nonAggregatableIndices = instance.nonAggregatableIndices();
        Map<String, Set<String>> meta = instance.meta();
        switch (between(0, 7)) {
        case 0:
            name += randomAlphaOfLengthBetween(1, 10);
            break;
        case 1:
            type += randomAlphaOfLengthBetween(1, 10);
            break;
        case 2:
            isSearchable = isSearchable == false;
            break;
        case 3:
            isAggregatable = isAggregatable == false;
            break;
        case 4:
            String[] newIndices;
            int startIndicesPos = 0;
            if (indices == null) {
                newIndices = new String[between(1, 10)];
            } else {
                newIndices = Arrays.copyOf(indices, indices.length + between(1, 10));
                startIndicesPos = indices.length;
            }
            for (int i = startIndicesPos; i < newIndices.length; i++) {
                newIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
            indices = newIndices;
            break;
        case 5:
            String[] newNonSearchableIndices;
            int startNonSearchablePos = 0;
            if (nonSearchableIndices == null) {
                newNonSearchableIndices = new String[between(1, 10)];
            } else {
                newNonSearchableIndices = Arrays.copyOf(nonSearchableIndices, nonSearchableIndices.length + between(1, 10));
                startNonSearchablePos = nonSearchableIndices.length;
            }
            for (int i = startNonSearchablePos; i < newNonSearchableIndices.length; i++) {
                newNonSearchableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
            nonSearchableIndices = newNonSearchableIndices;
            break;
        case 6:
            String[] newNonAggregatableIndices;
            int startNonAggregatablePos = 0;
            if (nonAggregatableIndices == null) {
                newNonAggregatableIndices = new String[between(1, 10)];
            } else {
                newNonAggregatableIndices = Arrays.copyOf(nonAggregatableIndices, nonAggregatableIndices.length + between(1, 10));
                startNonAggregatablePos = nonAggregatableIndices.length;
            }
            for (int i = startNonAggregatablePos; i < newNonAggregatableIndices.length; i++) {
                newNonAggregatableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
            nonAggregatableIndices = newNonAggregatableIndices;
            break;
        case 7:
            Map<String, Set<String>> newMeta;
            if (meta.isEmpty()) {
                newMeta = Map.of("foo", Set.of("bar"));
            } else {
                newMeta = Collections.emptyMap();
            }
            meta = newMeta;
            break;
        default:
            throw new AssertionError();
        }
        return new FieldCapabilities(name, type, isSearchable, isAggregatable, indices, nonSearchableIndices, nonAggregatableIndices, meta);
    }
}
