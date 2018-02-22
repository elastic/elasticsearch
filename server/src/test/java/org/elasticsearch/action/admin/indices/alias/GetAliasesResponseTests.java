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

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasMetaData.Builder;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class GetAliasesResponseTests extends AbstractStreamableXContentTestCase<GetAliasesResponse> {

    @Override
    protected GetAliasesResponse doParseInstance(XContentParser parser) {
        try {
            return GetAliasesResponse.fromXContent(parser);
        } catch (IOException e) {
            fail(e.getMessage());
            return null;
        }
    }

    @Override
    protected GetAliasesResponse createTestInstance() {
        return createTestItem();
    }

    @Override
    protected GetAliasesResponse createBlankInstance() {
        return new GetAliasesResponse(null);
    }

    @Override
    protected GetAliasesResponse mutateInstance(GetAliasesResponse response) {
        return new GetAliasesResponse(mutateAliases(response.getAliases()));
    }

    private static ImmutableOpenMap<String, List<AliasMetaData>> mutateAliases(ImmutableOpenMap<String, List<AliasMetaData>> aliases) {
        if (aliases.isEmpty()) {
            return addIndices(1, 3).build();
        }

        if (randomBoolean()) {
            ImmutableOpenMap.Builder<String, List<AliasMetaData>> builder = ImmutableOpenMap.builder(aliases);
            ImmutableOpenMap<String, List<AliasMetaData>> list = addIndices(1, 1).build();
            list.forEach(e -> builder.put(e.key, e.value));
            return builder.build();
        }

        Set<String> indices = new HashSet<>();
        Iterator<String> keys = aliases.keysIt();
        while (keys.hasNext()) {
            indices.add(keys.next());
        }

        List<String> indicesToBeModified = randomSubsetOf(randomIntBetween(1, indices.size()), indices);
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> builder = ImmutableOpenMap.builder();

        for (String index : indices) {
            List<AliasMetaData> list = new ArrayList<>(aliases.get(index));
            if (indicesToBeModified.contains(index)) {
                list.add(createAliasMetaData());
            }
            builder.put(index, list);
        }
        return builder.build();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.equals("") // do not add new indices
                || p.endsWith(".aliases") // do not add new alias
                || p.contains(".aliases."); // we should not be testing the AlilasMetaData.fromContent
    }

    private static GetAliasesResponse createTestItem() {
        return new GetAliasesResponse(addIndices(0, 5).build());
    }

    private static ImmutableOpenMap.Builder<String, List<AliasMetaData>> addIndices(int min, int max) {
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> builder = ImmutableOpenMap.builder();
        int indicesNum = randomIntBetween(min, max);
        for (int i = 0; i < indicesNum; i++) {
            String index = randomAlphaOfLength(5);
            List<AliasMetaData> aliasMetaData = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int a = 0; a < aliasesNum; a++) {
                aliasMetaData.add(createAliasMetaData());
            }
            builder.put(index, aliasMetaData);
        }
        return builder;
    }

    private static AliasMetaData createAliasMetaData() {
        Builder builder = AliasMetaData.builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.filter("{\"term\":{\"year\":2016}}");
        }
        return builder.build();
    }

}
