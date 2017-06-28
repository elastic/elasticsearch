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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class MapperTests extends ESTestCase {

    public void testSuccessfulBuilderContext() {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        ContentPath contentPath = new ContentPath(1);
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, contentPath);

        assertEquals(settings, context.indexSettings());
        assertEquals(contentPath, context.path());
    }

    public void testBuilderContextWithIndexSettingsAsNull() {
        NullPointerException e = expectThrows(NullPointerException.class, () -> new Mapper.BuilderContext(null, new ContentPath(1)));
    }

    public void testExceptionForIncludeInAll() throws IOException {
        XContentBuilder mapping = createMappingWithIncludeInAll();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();

        final MapperService currentMapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), settings, "test");
        Exception e = expectThrows(MapperParsingException.class, () ->
                currentMapperService.parse("type", new CompressedXContent(mapping.string()), true));
        assertEquals("[include_in_all] is not allowed for indices created on or after version 6.0.0 as [_all] is deprecated. " +
                        "As a replacement, you can use an [copy_to] on mapping fields to create your own catch all field.",
                e.getMessage());

        settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_3_0).build();

        // Create the mapping service with an older index creation version
        final MapperService oldMapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), settings, "test");
        // Should not throw an exception now
        oldMapperService.parse("type", new CompressedXContent(mapping.string()), true);
    }

    private static XContentBuilder createMappingWithIncludeInAll() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("a")
                .field("type", "text")
                .field("include_in_all", randomBoolean())
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }

}
