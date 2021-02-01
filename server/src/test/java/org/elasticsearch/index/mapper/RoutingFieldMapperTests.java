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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RoutingFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return RoutingFieldMapper.NAME;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("required", b -> b.field("required", true));
    }

    public void testRoutingMapper() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()),
            XContentType.JSON, "routing_value"));

        assertThat(doc.rootDoc().get("_routing"), equalTo("routing_value"));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(source(b -> b.field("_routing", "foo"))));

        assertThat(e.getCause().getMessage(),
                containsString("Field [_routing] is a metadata field and cannot be added inside a document"));
    }
}
