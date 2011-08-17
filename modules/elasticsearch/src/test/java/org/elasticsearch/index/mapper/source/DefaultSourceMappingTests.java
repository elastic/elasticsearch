/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.source;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTests;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class DefaultSourceMappingTests {

    @Test public void testDefaultMappingAndNoMapping() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = MapperTests.newParser().parse("my_type", null, defaultMapping);
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));

        try {
            mapper = MapperTests.newParser().parse(null, null, defaultMapping);
            assertThat(mapper.type(), equalTo("my_type"));
            assertThat(mapper.sourceMapper().enabled(), equalTo(false));
            assert false;
        } catch (MapperParsingException e) {
            // all is well
        }
    }

    @Test public void testDefaultMappingAndWithMappingOverride() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("enabled", true).endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = MapperTests.newParser().parse("my_type", mapping, defaultMapping);
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(true));
    }

    @Test public void testDefaultMappingAndNoMappingWithMapperService() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        MapperService mapperService = MapperTests.newMapperService();
        mapperService.add(MapperService.DEFAULT_MAPPING, defaultMapping);

        DocumentMapper mapper = mapperService.documentMapperWithAutoCreate("my_type");
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));
    }

    @Test public void testDefaultMappingAndWithMappingOverrideWithMapperService() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        MapperService mapperService = MapperTests.newMapperService();
        mapperService.add(MapperService.DEFAULT_MAPPING, defaultMapping);

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("enabled", true).endObject()
                .endObject().endObject().string();
        mapperService.add("my_type", mapping);

        DocumentMapper mapper = mapperService.documentMapper("my_type");
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(true));
    }
}
