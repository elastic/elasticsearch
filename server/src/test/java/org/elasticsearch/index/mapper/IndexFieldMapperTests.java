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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndexFieldMapperTests extends MapperServiceTestCase {

    public void testDefaultDisabledIndexMapper() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));
        assertThat(doc.rootDoc().get("_index"), nullValue());
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIndexNotConfigurable() {
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> createMapperService(topMapping(b -> b.startObject("_index").endObject())));
        assertThat(e.getMessage(), containsString("_index is not configurable"));
    }

}
