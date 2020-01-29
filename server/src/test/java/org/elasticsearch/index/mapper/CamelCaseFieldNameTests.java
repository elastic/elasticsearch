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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class CamelCaseFieldNameTests extends ESSingleNodeTestCase {
    public void testCamelCaseFieldNameStaysAsIs() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .endObject().endObject());

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();
        DocumentMapper documentMapper = index.mapperService().documentMapper();

        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "1",
                        BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                                .field("thisIsCamelCase", "value1")
                                .endObject()),
                        XContentType.JSON));

        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test")
            .setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();

        documentMapper = index.mapperService().documentMapper();
        assertNotNull(documentMapper.mappers().getMapper("thisIsCamelCase"));
        assertNull(documentMapper.mappers().getMapper("this_is_camel_case"));

        documentMapper = index.mapperService().documentMapperParser().parse("_doc", documentMapper.mappingSource());

        assertNotNull(documentMapper.mappers().getMapper("thisIsCamelCase"));
        assertNull(documentMapper.mappers().getMapper("this_is_camel_case"));
    }
}
