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

package org.elasticsearch.index.mapper.object;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.elasticsearch.index.mapper.MapperBuilders.*;

/**
 */
public class SimpleObjectMappingTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testDifferentInnerObjectTokenFailure() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        try {
            defaultMapper.parse("type", "1", new BytesArray(" {\n" +
                    "      \"object\": {\n" +
                    "        \"array\":[\n" +
                    "        {\n" +
                    "          \"object\": { \"value\": \"value\" }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"object\":\"value\"\n" +
                    "        }\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      \"value\":\"value\"\n" +
                    "    }"));
            fail();
        } catch (MapperParsingException e) {
            // all is well
        }
    }

    @Test
    public void testEmptyArrayProperties() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("properties").endArray()
                .endObject().endObject().string();
        createIndex("test").mapperService().documentMapperParser().parse(mapping);
    }

    public void testPathJustName() {
        IndexService indexService = createIndex("test");
        Settings settings = indexService.settingsService().getSettings();
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();
        // fails on the current version
        try {
            doc("test", settings, rootObject("person").add(object("name").pathType(ContentPath.Type.JUST_NAME).add(stringField("first").indexName("last")))).build(mapperParser);
            fail("path: just_name is not supported anymore");
        } catch (ElasticsearchIllegalArgumentException e) {
            // expected
        }
        // but succeeds on < 1.4.0
        Version v = Version.V_1_4_0;
        while (v.onOrAfter(Version.V_1_4_0)) {
            v = randomFrom(randomVersion());
        }
        settings = ImmutableSettings.builder().put(settings).put(IndexMetaData.SETTING_VERSION_CREATED, v).build();
        doc("test", settings, rootObject("person").add(object("name").pathType(ContentPath.Type.JUST_NAME).add(stringField("first").indexName("last")))).build(mapperParser);
    }
}
