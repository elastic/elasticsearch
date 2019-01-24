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

package org.elasticsearch.client.indices;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.elasticsearch.client.ccr.CcrStatsResponse;
import org.elasticsearch.client.ccr.CcrStatsResponseTests;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetIndexTemplatesResponseTests extends ESTestCase {
    
    static final String mappingString = "{\"properties\":{"
            + "\"f1\": {\"type\":\"text\"},"
            + "\"f2\": {\"type\":\"keyword\"}"
            + "}}";
    

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, GetIndexTemplatesResponseTests::createTestInstance, GetIndexTemplatesResponseTests::toXContent,
                GetIndexTemplatesResponse::fromXContent).supportsUnknownFields(false)
                .assertEqualsConsumer(GetIndexTemplatesResponseTests::assertEqualInstances)
                .shuffleFieldsExceptions(new String[] {"aliases", "mappings", "patterns", "settings"})
                .test();
    }

    private static void assertEqualInstances(GetIndexTemplatesResponse expectedInstance, GetIndexTemplatesResponse newInstance) {        
        assertEquals(expectedInstance, newInstance);
        // Check there's no doc types at the root of the mapping
        Map<String, Object> expectedMap = XContentHelper.convertToMap(
                new BytesArray(mappingString), true, XContentType.JSON).v2();
        for (IndexTemplateMetaData template : newInstance.getIndexTemplates()) {
            MappingMetaData mappingMD = template.mappings();
            if(mappingMD!=null) {
                Map<String, Object> mappingAsMap = mappingMD.sourceAsMap();
                assertEquals(expectedMap, mappingAsMap);
            }            
        }
    }    
    
    static GetIndexTemplatesResponse createTestInstance() {
        List<IndexTemplateMetaData> templates = new ArrayList<>();
        int numTemplates = between(0, 10);
        for (int t = 0; t < numTemplates; t++) {
            IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData.builder("template-" + t);
            templateBuilder.patterns(IntStream.range(0, between(1, 5)).mapToObj(i -> "pattern-" + i).collect(Collectors.toList()));
            int numAlias = between(0, 5);
            for (int i = 0; i < numAlias; i++) {
                templateBuilder.putAlias(AliasMetaData.builder(randomAlphaOfLengthBetween(1, 10)));
            }
            if (randomBoolean()) {
                templateBuilder.settings(Settings.builder().put("index.setting-1", randomLong()));
            }
            if (randomBoolean()) {
                templateBuilder.order(randomInt());
            }
            if (randomBoolean()) {
                templateBuilder.version(between(0, 100));
            }
            if (randomBoolean()) {
                try {
                    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(mappingString), true, XContentType.JSON).v2();
                    MappingMetaData mapping = new MappingMetaData(MapperService.SINGLE_MAPPING_NAME, map);
                    templateBuilder.mapping(mapping);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
            templates.add(templateBuilder.build());
        }
        return new GetIndexTemplatesResponse(templates);
    }

    // As the client class GetIndexTemplatesResponse doesn't have toXContent method, adding this method here only for the test
    static void toXContent(GetIndexTemplatesResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        for (IndexTemplateMetaData indexTemplateMetaData : response.getIndexTemplates()) {
            builder.startObject(indexTemplateMetaData.name());
            templateToXContent(indexTemplateMetaData, builder);
            builder.endObject();
        }
        builder.endObject();
    }

    private static void templateToXContent(IndexTemplateMetaData indexTemplateMetaData, XContentBuilder builder) throws IOException {
        builder.field("order", indexTemplateMetaData.order());
        if (indexTemplateMetaData.version() != null) {
            builder.field("version", indexTemplateMetaData.version());
        }
        builder.field("index_patterns", indexTemplateMetaData.patterns());

        builder.startObject("settings");
        indexTemplateMetaData.settings().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        MappingMetaData mappings = indexTemplateMetaData.mappings();
        if (mappings != null) {
            Map<String, Object> mappingsAsMap = mappings.getSourceAsMap();
            builder.field("mappings", mappingsAsMap);
        } else {
            builder.startObject("mappings").endObject();
        }

        builder.startObject("aliases");
        for (ObjectCursor<AliasMetaData> cursor : indexTemplateMetaData.aliases().values()) {
            AliasMetaData.Builder.toXContent(cursor.value, builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
    }

}
