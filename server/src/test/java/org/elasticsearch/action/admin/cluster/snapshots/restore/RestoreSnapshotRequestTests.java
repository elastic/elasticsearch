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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestoreSnapshotRequestTests extends AbstractWireSerializingTestCase<RestoreSnapshotRequest> {
    private RestoreSnapshotRequest randomState(RestoreSnapshotRequest instance) {
        if (randomBoolean()) {
            List<String> indices = new ArrayList<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                indices.add(randomAlphaOfLength(randomInt(3) + 2));
            }

            instance.indices(indices);
        }
        if (randomBoolean()) {
            instance.renamePattern(randomUnicodeOfLengthBetween(1, 100));
        }
        if (randomBoolean()) {
            instance.renameReplacement(randomUnicodeOfLengthBetween(1, 100));
        }
        instance.partial(randomBoolean());
        instance.includeAliases(randomBoolean());

        if (randomBoolean()) {
            Map<String, Object> indexSettings = new HashMap<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                indexSettings.put(randomAlphaOfLengthBetween(2, 5), randomAlphaOfLengthBetween(2, 5));
            }
            instance.indexSettings(indexSettings);
        }

        instance.includeGlobalState(randomBoolean());

        if (randomBoolean()) {
            Collection<IndicesOptions.WildcardStates> wildcardStates = randomSubsetOf(
                Arrays.asList(IndicesOptions.WildcardStates.values()));
            Collection<IndicesOptions.Option> options = randomSubsetOf(
                Arrays.asList(IndicesOptions.Option.ALLOW_NO_INDICES, IndicesOptions.Option.IGNORE_UNAVAILABLE));

            instance.indicesOptions(new IndicesOptions(
                options.isEmpty() ? IndicesOptions.Option.NONE : EnumSet.copyOf(options),
                wildcardStates.isEmpty() ? IndicesOptions.WildcardStates.NONE : EnumSet.copyOf(wildcardStates)));
        }

        instance.waitForCompletion(randomBoolean());

        if (randomBoolean()) {
            instance.masterNodeTimeout(randomTimeValue());
        }
        return instance;
    }

    @Override
    protected RestoreSnapshotRequest createTestInstance() {
        return randomState(new RestoreSnapshotRequest(randomAlphaOfLength(5), randomAlphaOfLength(10)));
    }

    @Override
    protected Writeable.Reader<RestoreSnapshotRequest> instanceReader() {
        return RestoreSnapshotRequest::new;
    }

    @Override
    protected RestoreSnapshotRequest mutateInstance(RestoreSnapshotRequest instance) throws IOException {
        RestoreSnapshotRequest copy = copyInstance(instance);
        // ensure that at least one property is different
        copy.repository("copied-" + instance.repository());
        return randomState(copy);
    }

    public void testSource() throws IOException {
        RestoreSnapshotRequest original = createTestInstance();
        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(Collections.emptyMap()));
        XContentParser parser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput());
        Map<String, Object> map = parser.mapOrdered();

        // we will only restore properties from the map that are contained in the request body. All other
        // properties are restored from the original (in the actual REST action this is restored from the
        // REST path and request parameters).
        RestoreSnapshotRequest processed = new RestoreSnapshotRequest(original.repository(), original.snapshot());
        processed.masterNodeTimeout(original.masterNodeTimeout());
        processed.waitForCompletion(original.waitForCompletion());

        processed.source(map);

        assertEquals(original, processed);
    }
}
