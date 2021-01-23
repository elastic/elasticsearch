/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.runtimefields;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.runtimefields.RuntimeFieldsFeatureSetUsage.RuntimeFieldStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RuntimeFieldsFeatureSetUsageTests extends AbstractWireSerializingTestCase<RuntimeFieldsFeatureSetUsage> {

    public void testToXContent() {
        Settings settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put("index.version.created", Version.CURRENT)
            .build();
        Script script1 = new Script("doc['field'] + doc.field + params._source.field");
        Script script2 = new Script("doc['field']");
        Script script3 = new Script("params._source.field + params._source.field \n + params._source.field");
        Script script4 = new Script("params._source.field");
        IndexMetadata meta = IndexMetadata.builder("index").settings(settings)
            .putMapping("{" +
                "  \"runtime\" : {" +
                "    \"keyword1\": {" +
                "      \"type\": \"keyword\"," +
                "      \"script\": " + Strings.toString(script1) +
                "    }," +
                "    \"keyword2\": {" +
                "      \"type\": \"keyword\"" +
                "    }," +
                "    \"object.keyword3\": {" +
                "      \"type\": \"keyword\"," +
                "      \"script\": " + Strings.toString(script2) +
                "    }," +
                "    \"long\": {" +
                "      \"type\": \"long\"," +
                "      \"script\": " + Strings.toString(script3) +
                "    }," +
                "    \"long2\": {" +
                "      \"type\": \"long\"," +
                "      \"script\": " + Strings.toString(script4) +
                "    }" +
                "  }," +
                "  \"properties\":{" +
                "    \"object\":{" +
                "      \"type\":\"object\"," +
                "      \"properties\":{" +
                "         \"keyword3\":{" +
                "           \"type\": \"keyword\"" +
                "         }" +
                "      }" +
                "    }" +
                "  }" +
                "}")
            .build();

        RuntimeFieldsFeatureSetUsage featureSetUsage = RuntimeFieldsFeatureSetUsage.fromMetadata(List.of(meta, meta));
        assertEquals("{\n" +
            "  \"available\" : true,\n" +
            "  \"enabled\" : true,\n" +
            "  \"field_types\" : [\n" +
            "    {\n" +
            "      \"name\" : \"keyword\",\n" +
            "      \"count\" : 6,\n" +
            "      \"index_count\" : 2,\n" +
            "      \"scriptless_count\" : 2,\n" +
            "      \"shadowed_count\" : 2,\n" +
            "      \"lang\" : [\n" +
            "        \"painless\"\n" +
            "      ],\n" +
            "      \"lines_max\" : 1,\n" +
            "      \"lines_total\" : 4,\n" +
            "      \"chars_max\" : 47,\n" +
            "      \"chars_total\" : 118,\n" +
            "      \"source_max\" : 1,\n" +
            "      \"source_total\" : 2,\n" +
            "      \"doc_max\" : 2,\n" +
            "      \"doc_total\" : 6\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\" : \"long\",\n" +
            "      \"count\" : 4,\n" +
            "      \"index_count\" : 2,\n" +
            "      \"scriptless_count\" : 0,\n" +
            "      \"shadowed_count\" : 0,\n" +
            "      \"lang\" : [\n" +
            "        \"painless\"\n" +
            "      ],\n" +
            "      \"lines_max\" : 2,\n" +
            "      \"lines_total\" : 6,\n" +
            "      \"chars_max\" : 68,\n" +
            "      \"chars_total\" : 176,\n" +
            "      \"source_max\" : 3,\n" +
            "      \"source_total\" : 8,\n" +
            "      \"doc_max\" : 0,\n" +
            "      \"doc_total\" : 0\n" +
            "    }\n" +
            "  ]\n" +
            "}", Strings.toString(featureSetUsage, true, true));
    }

    @Override
    protected RuntimeFieldsFeatureSetUsage createTestInstance() {
        int numItems = randomIntBetween(0, 10);
        List<RuntimeFieldStats> stats = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
            stats.add(randomRuntimeFieldStats("type" + i));
        }
        return new RuntimeFieldsFeatureSetUsage(stats);
    }

    private static RuntimeFieldStats randomRuntimeFieldStats(String type) {
        RuntimeFieldStats stats = new RuntimeFieldStats(type);
        if (randomBoolean()) {
            stats.update(randomIntBetween(1, 100), randomLongBetween(100, 1000), randomIntBetween(1, 10), randomIntBetween(1, 10));
        }
        return stats;
    }

    @Override
    protected RuntimeFieldsFeatureSetUsage mutateInstance(RuntimeFieldsFeatureSetUsage instance) throws IOException {
        List<RuntimeFieldStats> runtimeFieldStats = instance.getRuntimeFieldStats();
        if (runtimeFieldStats.size() == 0) {
            return new RuntimeFieldsFeatureSetUsage(Collections.singletonList(randomRuntimeFieldStats("type")));
        }
        List<RuntimeFieldStats> mutated = new ArrayList<>(runtimeFieldStats);
        mutated.remove(randomIntBetween(0, mutated.size() - 1));
        return new RuntimeFieldsFeatureSetUsage(mutated);
    }

    @Override
    protected Writeable.Reader<RuntimeFieldsFeatureSetUsage> instanceReader() {
        return RuntimeFieldsFeatureSetUsage::new;
    }
}
