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


package org.elasticsearch.index.mapper.core;

import org.elasticsearch.Version;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESTokenStreamTestCase.randomVersion;
import static org.hamcrest.core.IsEqual.equalTo;

public class MultiFieldCopyToMapperTests extends ESTestCase {

    public void testCopyToWithinMultiFieldWithRandomVersion() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("a")
            .field("type", "string")
            .endObject()
            .startObject("b")
            .field("type", "string")
            .startObject("fields")
            .startObject("c")
            .field("type", "string")
            .field("copy_to", "a")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        Version indexVersion = randomVersion();
        MapperService mapperService = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY, indexVersion);
        if (indexVersion.after(Version.V_2_1_0) ||
            (indexVersion.after(Version.V_2_0_1) && indexVersion.before(Version.V_2_1_0))) {
            try {
                mapperService.parse("type", new CompressedXContent(mapping.string()), true);
                fail("Parsing should throw an exception becasue the mapping contains a copy_to in a multi field");
            } catch (MapperParsingException e) {
                assertThat(e.getMessage(), equalTo("copy_to in multi fields is not allowed. Found the copy_to in field [c] which is within a multi field."));
            }
        } else {
            // this should just work
            mapperService.parse("type", new CompressedXContent(mapping.string()), true);
        }
    }
}
