/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;

public class ManifestTests extends ESTestCase {

    private Manifest copyState(Manifest state, boolean introduceErrors) {
        long generation = state.getGlobalGeneration();
        Map<Index, Long> indices = new HashMap<>(state.getIndexGenerations());
        if (introduceErrors) {
            switch (randomInt(3)) {
                case 0: {
                    generation = generation + 1;
                    break;
                }
                case 1: {
                    indices.remove(randomFrom(indices.keySet()));
                    break;
                }
                case 2: {
                    Tuple<Index, Long> indexEntry = randomIndexEntry();
                    indices.put(indexEntry.v1(), indexEntry.v2());
                    break;
                }
                case 3: {
                    Index index = randomFrom(indices.keySet());
                    indices.compute(index, (i, g) -> g + 1);
                    break;
                }
            }
        }
        return new Manifest(generation, indices);
    }

    private Tuple<Index, Long> randomIndexEntry() {
        final String name = randomAlphaOfLengthBetween(4, 15);
        final String uuid = UUIDs.randomBase64UUID();
        final Index index = new Index(name, uuid);
        final long indexGeneration = randomNonNegativeLong();
        return Tuple.tuple(index, indexGeneration);
    }

    private Manifest randomManifest() {
        long generation = randomNonNegativeLong();
        Map<Index, Long> indices = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            Tuple<Index, Long> indexEntry = randomIndexEntry();
            indices.put(indexEntry.v1(), indexEntry.v2());
        }
        return new Manifest(generation, indices);
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomManifest(), org -> copyState(org, false), org -> copyState(org, true));
    }

    public void testXContent() throws IOException {
        Manifest state = randomManifest();

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Manifest.FORMAT.toXContent(builder, state);
        builder.endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
            assertThat(Manifest.fromXContent(parser), equalTo(state));
        }
    }

    public void testEmptyManifest() {
        assertTrue(Manifest.empty().isEmpty());
        assertFalse(randomManifest().isEmpty());
    }
}