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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.Option;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent.MapParams;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateSnapshotRequestTests extends ESTestCase {

    // tests creating XContent and parsing with source(Map) equivalency
    public void testToXContent() throws IOException {
        String repo = randomAlphaOfLength(5);
        String snap = randomAlphaOfLength(10);

        CreateSnapshotRequest original = new CreateSnapshotRequest(repo, snap);

        if (randomBoolean()) {
            List<String> indices = new ArrayList<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                indices.add(randomAlphaOfLength(randomInt(3) + 2));
            }

            original.indices(indices);
        }

        if (randomBoolean()) {
            original.partial(randomBoolean());
        }

        if (randomBoolean()) {
            Map<String, Object> settings = new HashMap<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                settings.put(randomAlphaOfLength(randomInt(3) + 2), randomAlphaOfLength(randomInt(3) + 2));
            }

            original.settings(settings);
        }

        if (randomBoolean()) {
            original.includeGlobalState(randomBoolean());
        }

        if (randomBoolean()) {
            Collection<WildcardStates> wildcardStates = randomSubsetOf(Arrays.asList(WildcardStates.values()));
            Collection<Option> options = randomSubsetOf(Arrays.asList(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE));

            original.indicesOptions(new IndicesOptions(
                    options.isEmpty() ? Option.NONE : EnumSet.copyOf(options),
                    wildcardStates.isEmpty() ? WildcardStates.NONE : EnumSet.copyOf(wildcardStates)));
        }

        if (randomBoolean()) {
            original.waitForCompletion(randomBoolean());
        }

        if (randomBoolean()) {
            original.masterNodeTimeout("60s");
        }

        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new MapParams(Collections.emptyMap()));
        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput());
        Map<String, Object> map = parser.mapOrdered();
        CreateSnapshotRequest processed = new CreateSnapshotRequest((String)map.get("repository"), (String)map.get("snapshot"));
        processed.waitForCompletion((boolean)map.getOrDefault("wait_for_completion", false));
        processed.masterNodeTimeout((String)map.getOrDefault("master_node_timeout", "30s"));
        processed.source(map);

        assertEquals(original, processed);
    }
}
