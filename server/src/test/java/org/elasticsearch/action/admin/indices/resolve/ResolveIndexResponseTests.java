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

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedAlias;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedDataStream;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ResolveIndexResponseTests extends AbstractSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return Response.fromXContent(parser);
    }

    @Override
    protected Response createTestInstance() {
        final List<ResolvedIndex> indices = new ArrayList<>();
        final List<ResolvedAlias> aliases = new ArrayList<>();
        final List<ResolvedDataStream> dataStreams = new ArrayList<>();

        int num = randomIntBetween(0, 8);
        for (int k = 0; k < num; k++) {
            indices.add(createTestResolvedIndexInstance());
        }
        num = randomIntBetween(0, 8);
        for (int k = 0; k < num; k++) {
            aliases.add(createTestResolvedAliasInstance());
        }
        num = randomIntBetween(0, 8);
        for (int k = 0; k < num; k++) {
            dataStreams.add(createTestResolvedDataStreamInstance());
        }

        return new Response(indices, aliases, dataStreams);
    }

    private static ResolvedIndex createTestResolvedIndexInstance() {
        String name = randomAlphaOfLength(6);
        String[] aliases = randomStringArray(0, 5);
        String[] attributes = randomSubsetOf(List.of("open", "hidden", "frozen")).toArray(Strings.EMPTY_ARRAY);
        String dataStream = randomBoolean() ? randomAlphaOfLength(6) : null;

        return new ResolvedIndex(name, aliases, attributes, dataStream);
    }

    private static ResolvedAlias createTestResolvedAliasInstance() {
        String name = randomAlphaOfLength(6);
        String[] indices = randomStringArray(1, 6);
        return new ResolvedAlias(name, indices);
    }

    private static ResolvedDataStream createTestResolvedDataStreamInstance() {
        String name = randomAlphaOfLength(6);
        String[] backingIndices = randomStringArray(1, 6);
        String timestampField = randomAlphaOfLength(6);
        return new ResolvedDataStream(name, backingIndices, timestampField);
    }

    private static String[] randomStringArray(int minLength, int maxLength) {
        int num = randomIntBetween(minLength, maxLength);
        String[] stringArray = new String[num];
        for (int k = 0; k < num; k++) {
            stringArray[k] = randomAlphaOfLength(6);
        }
        return stringArray;
    }
}
