package org.elasticsearch.index.mapper.null_value;

import org.elasticsearch.common.compress.CompressedXContent;

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


import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class NullValueTests extends ESSingleNodeTestCase {
    public void testNullNullValue() throws Exception {
        IndexService indexService = createIndex("test", Settings.settingsBuilder().build());
        String[] typesToTest = {"integer", "long", "double", "float", "short", "date", "ip", "string", "boolean", "byte"};

        for (String type : typesToTest) {
            String mapping = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("type")
                            .startObject("properties")
                                .startObject("numeric")
                                    .field("type", type)
                                    .field("null_value", (String) null)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject().string();

            try {
                indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
                fail("Test should have failed because [null_value] was null.");
            } catch (MapperParsingException e) {
                assertThat(e.getMessage(), equalTo("Property [null_value] cannot be null."));
            }
        }
    }
}
