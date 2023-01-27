/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesNodeRequestTests extends AbstractWireSerializingTestCase<FieldCapabilitiesNodeRequest> {

    @Override
    protected FieldCapabilitiesNodeRequest createTestInstance() {
        List<ShardId> randomShards = randomShardIds(randomIntBetween(1, 5));
        String[] randomFields = randomFields(randomIntBetween(1, 20));
        String[] randomFilter = randomBoolean() ? Strings.EMPTY_ARRAY : new String[] { "-nested" };
        String[] randomTypeFilter = randomBoolean() ? Strings.EMPTY_ARRAY : new String[] { "keyword" };
        OriginalIndices originalIndices = randomOriginalIndices(randomIntBetween(0, 20));

        QueryBuilder indexFilter = randomBoolean() ? QueryBuilders.termQuery("field", randomAlphaOfLength(5)) : null;
        long nowInMillis = randomLong();

        Map<String, Object> runtimeFields = randomBoolean()
            ? Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5))
            : null;

        return new FieldCapabilitiesNodeRequest(
            randomShards,
            randomFields,
            randomFilter,
            randomTypeFilter,
            originalIndices,
            indexFilter,
            nowInMillis,
            runtimeFields
        );
    }

    private List<ShardId> randomShardIds(int numShards) {
        List<ShardId> randomShards = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            randomShards.add(new ShardId("index", randomAlphaOfLength(10), i));
        }
        return randomShards;
    }

    private String[] randomFields(int numFields) {
        String[] randomFields = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            randomFields[i] = randomAlphaOfLengthBetween(5, 10);
        }
        return randomFields;
    }

    private OriginalIndices randomOriginalIndices(int numIndices) {
        String[] randomIndices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            randomIndices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        IndicesOptions indicesOptions = randomBoolean() ? IndicesOptions.strictExpand() : IndicesOptions.lenientExpandOpen();
        return new OriginalIndices(randomIndices, indicesOptions);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesNodeRequest> instanceReader() {
        return FieldCapabilitiesNodeRequest::new;
    }

    @Override
    protected FieldCapabilitiesNodeRequest mutateInstance(FieldCapabilitiesNodeRequest instance) {
        switch (random().nextInt(7)) {
            case 0 -> {
                List<ShardId> shardIds = randomShardIds(instance.shardIds().size() + 1);
                return new FieldCapabilitiesNodeRequest(
                    shardIds,
                    instance.fields(),
                    instance.filters(),
                    instance.allowedTypes(),
                    instance.originalIndices(),
                    instance.indexFilter(),
                    instance.nowInMillis(),
                    instance.runtimeFields()
                );
            }
            case 1 -> {
                String[] fields = randomFields(instance.fields().length + 2);
                return new FieldCapabilitiesNodeRequest(
                    instance.shardIds(),
                    fields,
                    instance.filters(),
                    instance.allowedTypes(),
                    instance.originalIndices(),
                    instance.indexFilter(),
                    instance.nowInMillis(),
                    instance.runtimeFields()
                );
            }
            case 2 -> {
                OriginalIndices originalIndices = randomOriginalIndices(instance.indices().length + 1);
                return new FieldCapabilitiesNodeRequest(
                    instance.shardIds(),
                    instance.fields(),
                    instance.filters(),
                    instance.allowedTypes(),
                    originalIndices,
                    instance.indexFilter(),
                    instance.nowInMillis(),
                    instance.runtimeFields()
                );
            }
            case 3 -> {
                QueryBuilder indexFilter = instance.indexFilter() == null ? QueryBuilders.matchAllQuery() : null;
                return new FieldCapabilitiesNodeRequest(
                    instance.shardIds(),
                    instance.fields(),
                    instance.filters(),
                    instance.allowedTypes(),
                    instance.originalIndices(),
                    indexFilter,
                    instance.nowInMillis(),
                    instance.runtimeFields()
                );
            }
            case 4 -> {
                long nowInMillis = instance.nowInMillis() + 100;
                return new FieldCapabilitiesNodeRequest(
                    instance.shardIds(),
                    instance.fields(),
                    instance.filters(),
                    instance.allowedTypes(),
                    instance.originalIndices(),
                    instance.indexFilter(),
                    nowInMillis,
                    instance.runtimeFields()
                );
            }
            case 5 -> {
                Map<String, Object> runtimeFields = instance.runtimeFields() == null
                    ? Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5))
                    : null;
                return new FieldCapabilitiesNodeRequest(
                    instance.shardIds(),
                    instance.fields(),
                    instance.filters(),
                    instance.allowedTypes(),
                    instance.originalIndices(),
                    instance.indexFilter(),
                    instance.nowInMillis(),
                    runtimeFields
                );
            }
            case 6 -> {
                String[] randomFilter = instance.filters().length > 0 ? Strings.EMPTY_ARRAY : new String[] { "-nested" };
                return new FieldCapabilitiesNodeRequest(
                    instance.shardIds(),
                    instance.fields(),
                    randomFilter,
                    instance.allowedTypes(),
                    instance.originalIndices(),
                    instance.indexFilter(),
                    instance.nowInMillis(),
                    instance.runtimeFields()
                );
            }
            case 7 -> {
                String[] randomType = instance.allowedTypes().length > 0 ? Strings.EMPTY_ARRAY : new String[] { "text" };
                return new FieldCapabilitiesNodeRequest(
                    instance.shardIds(),
                    instance.fields(),
                    instance.filters(),
                    randomType,
                    instance.originalIndices(),
                    instance.indexFilter(),
                    instance.nowInMillis(),
                    instance.runtimeFields()
                );
            }
            default -> throw new IllegalStateException("The test should only allow 7 parameters mutated");
        }
    }

    public void testDescription() {
        FieldCapabilitiesNodeRequest r1 = new FieldCapabilitiesNodeRequest(
            List.of(new ShardId("index-1", "n/a", 0), new ShardId("index-2", "n/a", 3)),
            new String[] { "field-1", "field-2" },
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            randomOriginalIndices(1),
            null,
            randomNonNegativeLong(),
            Map.of()
        );
        assertThat(r1.getDescription(), equalTo("shards[[index-1][0],[index-2][3]], fields[field-1,field-2], filters[], types[]"));

        FieldCapabilitiesNodeRequest r2 = new FieldCapabilitiesNodeRequest(
            List.of(new ShardId("index-1", "n/a", 0)),
            new String[] { "*" },
            new String[] { "-nested", "-metadata" },
            Strings.EMPTY_ARRAY,
            randomOriginalIndices(1),
            null,
            randomNonNegativeLong(),
            Map.of()
        );
        assertThat(r2.getDescription(), equalTo("shards[[index-1][0]], fields[*], filters[-nested,-metadata], types[]"));
    }
}
