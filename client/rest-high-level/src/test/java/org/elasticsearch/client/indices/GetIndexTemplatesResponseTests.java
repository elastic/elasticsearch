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

import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.ToXContent.MapParams;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
        
        //Create a server-side counterpart for the client-side class and call toXContent on it
        
        List<org.elasticsearch.cluster.metadata.IndexTemplateMetaData> serverIndexTemplates = new ArrayList<>();
        List<IndexTemplateMetaData> clientIndexTemplates = response.getIndexTemplates();
        for (IndexTemplateMetaData clientITMD : clientIndexTemplates) {
            org.elasticsearch.cluster.metadata.IndexTemplateMetaData.Builder serverTemplateBuilder = 
                    org.elasticsearch.cluster.metadata.IndexTemplateMetaData.builder(clientITMD.name());

            serverTemplateBuilder.patterns(clientITMD.patterns());

            Iterator<AliasMetaData> aliases = clientITMD.aliases().valuesIt();
            aliases.forEachRemaining((a)->serverTemplateBuilder.putAlias(a));
            
            serverTemplateBuilder.settings(clientITMD.settings());
            serverTemplateBuilder.order(clientITMD.order());
            serverTemplateBuilder.version(clientITMD.version());
            if (clientITMD.mappings() != null) {
                serverTemplateBuilder.putMapping(MapperService.SINGLE_MAPPING_NAME, clientITMD.mappings().source());
            }
            serverIndexTemplates.add(serverTemplateBuilder.build());

        }
        org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse serverResponse = new        
                org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse(serverIndexTemplates);
        MapParams params =
                new MapParams(Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "false"));        
        serverResponse.toXContent(builder, params);
    }
}
