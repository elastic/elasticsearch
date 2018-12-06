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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.IsEqual.equalTo;

public class MultiFieldCopyToMapperTests extends ESTestCase {

    public void testExceptionForCopyToInMultiFields() throws IOException {
        XContentBuilder mapping = createMappinmgWithCopyToInMultiField();

        // first check that for newer versions we throw exception if copy_to is found within multi field
        MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), Settings.EMPTY, "test");
        try {
            mapperService.parse("type", new CompressedXContent(Strings.toString(mapping)), true);
            fail("Parsing should throw an exception because the mapping contains a copy_to in a multi field");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("copy_to in multi fields is not allowed. Found the copy_to in field [c]"
                + " which is within a multi field."));
        }
    }

    private static XContentBuilder createMappinmgWithCopyToInMultiField() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("a")
            .field("type", "text")
            .endObject()
            .startObject("b")
            .field("type", "text")
            .startObject("fields")
            .startObject("c")
            .field("type", "text")
            .field("copy_to", "a")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        return mapping;
    }

}
