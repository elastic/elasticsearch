/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class PreviewTransformResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
                this::createTestInstance,
                this::toXContent,
                PreviewTransformResponse::fromXContent)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(path -> path.isEmpty() == false)
                .test();
    }

    private PreviewTransformResponse createTestInstance() {
        int numDocs = randomIntBetween(5, 10);
        List<Map<String, Object>> docs = new ArrayList<>(numDocs);
        for (int i=0; i<numDocs; i++) {
            int numFields = randomIntBetween(1, 4);
            Map<String, Object> doc = new HashMap<>();
            for (int j=0; j<numFields; j++) {
                doc.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            docs.add(doc);
        }
        int numMappingEntries = randomIntBetween(5, 10);
        Map<String, Object> mappings = new HashMap<>(numMappingEntries);
        for (int i = 0; i < numMappingEntries; i++) {
            mappings.put(randomAlphaOfLength(10), Map.of("type", randomAlphaOfLength(10)));
        }

        return new PreviewTransformResponse(docs, mappings);
    }

    private void toXContent(PreviewTransformResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray("preview");
        for (Map<String, Object> doc : response.getDocs()) {
            builder.map(doc);
        }
        builder.endArray();
        builder.field("mappings", response.getMappings());
        builder.endObject();
    }
}
