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

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.AliasMetadata.Builder;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class GetAliasesResponseTests extends AbstractWireSerializingTestCase<GetAliasesResponse> {

    @Override
    protected GetAliasesResponse createTestInstance() {
        return createTestItem();
    }

    @Override
    protected Writeable.Reader<GetAliasesResponse> instanceReader() {
        return GetAliasesResponse::new;
    }

    @Override
    protected GetAliasesResponse mutateInstance(GetAliasesResponse response) {
        return new GetAliasesResponse(mutateAliases(response.getAliases()));
    }

    private static ImmutableOpenMap<String, List<AliasMetadata>> mutateAliases(ImmutableOpenMap<String, List<AliasMetadata>> aliases) {
        if (aliases.isEmpty()) {
            return createIndicesAliasesMap(1, 3).build();
        }

        if (randomBoolean()) {
            ImmutableOpenMap.Builder<String, List<AliasMetadata>> builder = ImmutableOpenMap.builder(aliases);
            ImmutableOpenMap<String, List<AliasMetadata>> list = createIndicesAliasesMap(1, 2).build();
            list.forEach(e -> builder.put(e.key, e.value));
            return builder.build();
        }

        Set<String> indices = new HashSet<>();
        Iterator<String> keys = aliases.keysIt();
        while (keys.hasNext()) {
            indices.add(keys.next());
        }

        List<String> indicesToBeModified = randomSubsetOf(randomIntBetween(1, indices.size()), indices);
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> builder = ImmutableOpenMap.builder();

        for (String index : indices) {
            List<AliasMetadata> list = new ArrayList<>(aliases.get(index));
            if (indicesToBeModified.contains(index)) {
                if (randomBoolean() || list.isEmpty()) {
                    list.add(createAliasMetadata());
                } else {
                    int aliasIndex = randomInt(list.size() - 1);
                    AliasMetadata aliasMetadata = list.get(aliasIndex);
                    list.add(aliasIndex, mutateAliasMetadata(aliasMetadata));
                }
            }
            builder.put(index, list);
        }
        return builder.build();
    }

    private static GetAliasesResponse createTestItem() {
        return new GetAliasesResponse(createIndicesAliasesMap(0, 5).build());
    }

    private static ImmutableOpenMap.Builder<String, List<AliasMetadata>> createIndicesAliasesMap(int min, int max) {
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> builder = ImmutableOpenMap.builder();
        int indicesNum = randomIntBetween(min, max);
        for (int i = 0; i < indicesNum; i++) {
            String index = randomAlphaOfLength(5);
            List<AliasMetadata> aliasMetadata = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int alias = 0; alias < aliasesNum; alias++) {
                aliasMetadata.add(createAliasMetadata());
            }
            builder.put(index, aliasMetadata);
        }
        return builder;
    }

    public static AliasMetadata createAliasMetadata() {
        Builder builder = AliasMetadata.builder(randomAlphaOfLengthBetween(3, 10));
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

    public static AliasMetadata mutateAliasMetadata(AliasMetadata alias) {
        boolean changeAlias = randomBoolean();
        AliasMetadata.Builder builder = AliasMetadata.builder(changeAlias ? randomAlphaOfLengthBetween(2, 5) : alias.getAlias());
        builder.searchRouting(alias.searchRouting());
        builder.indexRouting(alias.indexRouting());
        builder.filter(alias.filter());

        if (false == changeAlias) {
            if (randomBoolean()) {
                builder.searchRouting(alias.searchRouting() + randomAlphaOfLengthBetween(1, 3));
            } else {
                builder.indexRouting(alias.indexRouting() + randomAlphaOfLengthBetween(1, 3));
            }
        }
        return builder.build();
    }
}
