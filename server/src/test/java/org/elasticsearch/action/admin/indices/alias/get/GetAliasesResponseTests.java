/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.AliasMetadata.Builder;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

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
        return new GetAliasesResponse(
            mutateAliases(response.getAliases()),
            randomMap(5, 5, () -> new Tuple<>(randomAlphaOfLength(4), randomList(5, DataStreamTestHelper::randomAliasInstance)))
        );
    }

    private static Map<String, List<AliasMetadata>> mutateAliases(Map<String, List<AliasMetadata>> aliases) {
        if (aliases.isEmpty()) {
            return Collections.unmodifiableMap(createIndicesAliasesMap(1, 3));
        }

        if (randomBoolean()) {
            Map<String, List<AliasMetadata>> builder = new HashMap<>(aliases);
            builder.putAll(createIndicesAliasesMap(1, 2));
            return Collections.unmodifiableMap(builder);
        }

        List<String> indicesToBeModified = randomSubsetOf(randomIntBetween(1, aliases.size()), aliases.keySet());
        Map<String, List<AliasMetadata>> builder = new HashMap<>();

        for (var alias : aliases.entrySet()) {
            List<AliasMetadata> list = new ArrayList<>(alias.getValue());
            if (indicesToBeModified.contains(alias.getKey())) {
                if (randomBoolean() || list.isEmpty()) {
                    list.add(createAliasMetadata());
                } else {
                    int aliasIndex = randomInt(list.size() - 1);
                    AliasMetadata aliasMetadata = list.get(aliasIndex);
                    list.add(aliasIndex, mutateAliasMetadata(aliasMetadata));
                }
            }
            builder.put(alias.getKey(), Collections.unmodifiableList(list));
        }
        return Collections.unmodifiableMap(builder);
    }

    private static GetAliasesResponse createTestItem() {
        return new GetAliasesResponse(
            mutateAliases(createIndicesAliasesMap(0, 5)),
            randomMap(5, 5, () -> new Tuple<>(randomAlphaOfLength(4), randomList(5, DataStreamTestHelper::randomAliasInstance)))
        );
    }

    private static Map<String, List<AliasMetadata>> createIndicesAliasesMap(int min, int max) {
        Map<String, List<AliasMetadata>> builder = new HashMap<>();
        int indicesNum = randomIntBetween(min, max);
        for (int i = 0; i < indicesNum; i++) {
            String index = randomAlphaOfLength(5);
            List<AliasMetadata> aliasMetadata = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int alias = 0; alias < aliasesNum; alias++) {
                aliasMetadata.add(createAliasMetadata());
            }
            builder.put(index, Collections.unmodifiableList(aliasMetadata));
        }
        return builder;
    }

    public static AliasMetadata createAliasMetadata() {
        return createAliasMetadata(s -> false);
    }

    public static AliasMetadata createAliasMetadata(Predicate<String> t) {
        Builder builder = AliasMetadata.builder(randomValueOtherThanMany(t, () -> randomAlphaOfLengthBetween(3, 10)));
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
