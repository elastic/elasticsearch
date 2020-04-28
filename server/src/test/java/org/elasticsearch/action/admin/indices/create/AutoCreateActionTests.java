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
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public class AutoCreateActionTests extends ESTestCase {

    public void testResolveAutoCreateDataStreams() {
        Metadata metadata;
        {
            Metadata.Builder mdBuilder = new Metadata.Builder();
            IndexTemplateV2.DataStreamTemplate dataStreamTemplate = new IndexTemplateV2.DataStreamTemplate("@timestamp");
            mdBuilder.put("1", new IndexTemplateV2(List.of("legacy-logs-*"), null, null, 10L, null, null, null));
            mdBuilder.put("2", new IndexTemplateV2(List.of("logs-*"), null, null, 20L, null, null, dataStreamTemplate));
            mdBuilder.put("3", new IndexTemplateV2(List.of("logs-foobar"), null, null, 30L, null, null, dataStreamTemplate));
            metadata = mdBuilder.build();
        }

        Map<String, IndexTemplateV2.DataStreamTemplate> result = AutoCreateAction.resolveAutoCreateDataStreams(metadata, Set.of());
        assertThat(result, anEmptyMap());

        Set<String> autoCreateIndices = new HashSet<>(Set.of("logs-foobar", "logs-barbaz"));
        result = AutoCreateAction.resolveAutoCreateDataStreams(metadata, autoCreateIndices);
        assertThat(autoCreateIndices, empty());
        assertThat(result, aMapWithSize(2));
        assertThat(result, hasKey("logs-foobar"));
        assertThat(result, hasKey("logs-barbaz"));

        // An index that matches with a template without a data steam definition
        autoCreateIndices = new HashSet<>(Set.of("logs-foobar", "logs-barbaz", "legacy-logs-foobaz"));
        result = AutoCreateAction.resolveAutoCreateDataStreams(metadata, autoCreateIndices);
        assertThat(autoCreateIndices, hasSize(1));
        assertThat(autoCreateIndices, hasItem("legacy-logs-foobaz"));
        assertThat(result, aMapWithSize(2));
        assertThat(result, hasKey("logs-foobar"));
        assertThat(result, hasKey("logs-barbaz"));

        // An index that doesn't match with an index template
        autoCreateIndices = new HashSet<>(Set.of("logs-foobar", "logs-barbaz", "my-index"));
        result = AutoCreateAction.resolveAutoCreateDataStreams(metadata, autoCreateIndices);
        assertThat(autoCreateIndices, hasSize(1));
        assertThat(autoCreateIndices, hasItem("my-index"));
        assertThat(result, aMapWithSize(2));
        assertThat(result, hasKey("logs-foobar"));
        assertThat(result, hasKey("logs-barbaz"));
    }

}
