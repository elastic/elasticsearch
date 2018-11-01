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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MetaStateTests extends ESTestCase {

    private MetaState copyState(MetaState state, boolean introduceErrors){
        long generation = state.getGlobalStateGeneration();
        Map<Index, Long> indices = new HashMap<>(state.getIndices());
        if (introduceErrors) {
            if (randomBoolean()){
                generation = generation + 1;
            } else {
                indices.remove(randomFrom(indices.keySet()));
            }
        }
        return new MetaState(generation, indices);
    }

    private MetaState createRandomState() {
        long generation = randomNonNegativeLong();
        Map<Index, Long> indices = new HashMap<>();
        for (int i=0; i<randomIntBetween(1,5); i++) {
            final String name = randomAlphaOfLengthBetween(4, 15);
            final String uuid = UUIDs.randomBase64UUID();
            final Index index = new Index(name, uuid);
            final long indexGeneration =  randomNonNegativeLong();
            indices.put(index, indexGeneration);
        }
        return new MetaState(generation, indices);
    }

    public void testEquals(){
        MetaState state = createRandomState();
        MetaState copy = copyState(state, false);
        assertEquals(state, copy);
    }

    public void testNonEquals(){
        MetaState state = createRandomState();
        MetaState copy = copyState(state, true);
        assertNotEquals(state, copy);
    }

    public void testXContent() throws IOException {
        MetaState state = createRandomState();

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        MetaState.FORMAT.toXContent(builder, state);
        builder.endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
            assertThat(MetaState.fromXContent(parser), equalTo(state));
        }
    }
}
