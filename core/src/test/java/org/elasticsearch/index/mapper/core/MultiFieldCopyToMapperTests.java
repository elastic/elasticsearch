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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.IsEqual.equalTo;

public class MultiFieldCopyToMapperTests extends ESTestCase {

    public void testExceptionForCopyToInMultiFields() throws IOException {
        XContentBuilder mapping = createMappinmgWithCopyToInMultiField();
        Tuple<List<Version>, List<Version>> versionsWithAndWithoutExpectedExceptions = versionsWithAndWithoutExpectedExceptions();

        // first check that for newer versions we throw exception if copy_to is found withing multi field
        Version indexVersion = randomFrom(versionsWithAndWithoutExpectedExceptions.v1());
        MapperService mapperService = MapperTestUtils.newMapperService(createTempDir(), Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, indexVersion).build());
        try {
            mapperService.parse("type", new CompressedXContent(mapping.string()), true);
            fail("Parsing should throw an exception because the mapping contains a copy_to in a multi field");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("copy_to in multi fields is not allowed. Found the copy_to in field [c] which is within a multi field."));
        }

        // now test that with an older version the parsing just works
        indexVersion = randomFrom(versionsWithAndWithoutExpectedExceptions.v2());
        mapperService = MapperTestUtils.newMapperService(createTempDir(), Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, indexVersion).build());
        DocumentMapper documentMapper = mapperService.parse("type", new CompressedXContent(mapping.string()), true);
        assertFalse(documentMapper.mapping().toString().contains("copy_to"));
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

    // returns a tuple where
    // v1 is a list of versions for which we expect an exception when a copy_to in multi fields is found and
    // v2 is older versions where we throw no exception and we just log a warning
    private static Tuple<List<Version>, List<Version>> versionsWithAndWithoutExpectedExceptions() {
        List<Version> versionsWithException = new ArrayList<>();
        List<Version> versionsWithoutException = new ArrayList<>();
        for (Version version : VersionUtils.allVersions()) {
            if (version.after(Version.V_2_1_0) ||
                (version.after(Version.V_2_0_1) && version.before(Version.V_2_1_0))) {
                versionsWithException.add(version);
            } else {
                versionsWithoutException.add(version);
            }
        }
        return new Tuple<>(versionsWithException, versionsWithoutException);
    }
}
