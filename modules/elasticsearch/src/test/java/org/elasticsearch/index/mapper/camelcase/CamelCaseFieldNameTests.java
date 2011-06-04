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

package org.elasticsearch.index.mapper.camelcase;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperTests;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class CamelCaseFieldNameTests {

    @Test public void testCamelCaseFieldNameStaysAsIs() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        DocumentMapper documentMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("thisIsCamelCase", "value1")
                .endObject().copiedBytes());

        assertThat(documentMapper.mappers().indexName("thisIsCamelCase").isEmpty(), equalTo(false));
        assertThat(documentMapper.mappers().indexName("this_is_camel_case"), nullValue());

        documentMapper.refreshSource();
        documentMapper = MapperTests.newParser().parse(documentMapper.mappingSource().string());

        assertThat(documentMapper.mappers().indexName("thisIsCamelCase").isEmpty(), equalTo(false));
        assertThat(documentMapper.mappers().indexName("this_is_camel_case"), nullValue());
    }
}
