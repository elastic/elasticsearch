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

package org.elasticsearch.index.mapper.camelcase;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;

/**
 *
 */
public class CamelCaseFieldNameTests extends ESSingleNodeTestCase {
    public void testCamelCaseFieldNameStaysAsIs() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();
        DocumentMapper documentMapper = index.mapperService().documentMapper("type");

        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
                .field("thisIsCamelCase", "value1")
                .endObject().bytes());

        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test").setType("type").setSource(doc.dynamicMappingsUpdate().toString()).get();

        documentMapper = index.mapperService().documentMapper("type");
        assertNotNull(documentMapper.mappers().getMapper("thisIsCamelCase"));
        assertNull(documentMapper.mappers().getMapper("this_is_camel_case"));

        documentMapper = index.mapperService().documentMapperParser().parse("type", documentMapper.mappingSource());

        assertNotNull(documentMapper.mappers().getMapper("thisIsCamelCase"));
        assertNull(documentMapper.mappers().getMapper("this_is_camel_case"));
    }
}
