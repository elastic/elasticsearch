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

package org.elasticsearch.search.builder;


import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.builder.SearchSourceBuilderTests.createSearchSourceBuilder;

public class SearchSourceBuilderTest extends ESTestCase {
    private static NamedWriteableRegistry namedWriteableRegistry;

    @BeforeClass
    public static void beforeClass() {
        IndicesModule indicesModule = new IndicesModule(emptyList()) {
            @Override
            protected void configure() {
                bindMapperExtension();
            }
        };
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList()) {
            @Override
            protected void configureSearch() {
                // Skip me
            }
        };
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testSearchRequestBuilderSerializationWithIndexBoost() throws Exception {
        SearchSourceBuilder searchSourceBuilder = createSearchSourceBuilder();
        createIndexBoost(searchSourceBuilder);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            searchSourceBuilder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                SearchSourceBuilder deserializedSearchSourceBuilder = new SearchSourceBuilder(in);
                BytesStreamOutput deserializedOutput = new BytesStreamOutput();
                deserializedSearchSourceBuilder.writeTo(deserializedOutput);
                assertEquals(output.bytes(), deserializedOutput.bytes());
            }
        }
    }

    private void createIndexBoost(SearchSourceBuilder searchSourceBuilder) {
        int indexBoostSize = randomIntBetween(1, 10);
        for (int i = 0; i < indexBoostSize; i++) {
            searchSourceBuilder.indexBoost(randomAsciiOfLengthBetween(5, 20), randomFloat() * 10);
        }
    }
}
