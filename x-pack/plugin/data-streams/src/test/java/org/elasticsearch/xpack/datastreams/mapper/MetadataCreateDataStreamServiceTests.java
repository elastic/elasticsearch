/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.datastreams.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.cluster.DataStreamTestHelper.generateMapping;
import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.validateTimestampFieldMapping;
import static org.hamcrest.Matchers.equalTo;

public class MetadataCreateDataStreamServiceTests extends ESTestCase {

    public void testValidateTimestampFieldMapping() throws Exception {
        String mapping = generateMapping("@timestamp", "date");
        validateTimestampFieldMapping("@timestamp", createMapperService(mapping));
        mapping = generateMapping("@timestamp", "date_nanos");
        validateTimestampFieldMapping("@timestamp", createMapperService(mapping));
    }

    public void testValidateTimestampFieldMappingNoFieldMapping() {
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> validateTimestampFieldMapping("@timestamp", createMapperService("{}"))
        );
        assertThat(e.getMessage(), equalTo("[_data_stream_timestamp] meta field has been disabled"));
        String mapping1 = "{\n"
            + "      \"_data_stream_timestamp\": {\n"
            + "        \"enabled\": false\n"
            + "      },"
            + "      \"properties\": {\n"
            + "        \"@timestamp\": {\n"
            + "          \"type\": \"date\"\n"
            + "        }\n"
            + "      }\n"
            + "    }";
        e = expectThrows(IllegalStateException.class, () -> validateTimestampFieldMapping("@timestamp", createMapperService(mapping1)));
        assertThat(e.getMessage(), equalTo("[_data_stream_timestamp] meta field has been disabled"));

        String mapping2 = generateMapping("@timestamp2", "date");
        e = expectThrows(IllegalArgumentException.class, () -> validateTimestampFieldMapping("@timestamp", createMapperService(mapping2)));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] does not exist"));
    }

    public void testValidateTimestampFieldMappingInvalidFieldType() {
        String mapping = generateMapping("@timestamp", "keyword");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> validateTimestampFieldMapping("@timestamp", createMapperService(mapping))
        );
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] is of type [keyword], " + "but [date,date_nanos] is expected")
        );
    }

    MapperService createMapperService(String mapping) throws IOException {
        String indexName = "test";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
            .putMapping(mapping)
            .build();
        IndicesModule indicesModule = new IndicesModule(List.of(new DataStreamsPlugin()));
        MapperService mapperService = MapperTestUtils.newMapperService(
            xContentRegistry(),
            createTempDir(),
            Settings.EMPTY,
            indicesModule,
            indexName
        );
        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        return mapperService;
    }

}
