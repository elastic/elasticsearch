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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;

public class LegacyNestedObjectMapperTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testLimitOfNestedFieldsWithMultiTypePerIndex() throws Exception {
        Function<String, String> mapping = type -> {
            try {
                return Strings.toString(XContentFactory.jsonBuilder().startObject().startObject(type).startObject("properties")
                        .startObject("nested1").field("type", "nested").startObject("properties")
                        .startObject("nested2").field("type", "nested")
                        .endObject().endObject().endObject()
                        .endObject().endObject().endObject());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        MapperService mapperService = createIndex("test4", Settings.builder()
                .put("index.version.created", Version.V_5_6_0)
                .put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 2).build()).mapperService();
        mapperService.merge("type1", new CompressedXContent(mapping.apply("type1")), MergeReason.MAPPING_UPDATE, false);
        // merging same fields, but different type is ok
        mapperService.merge("type2", new CompressedXContent(mapping.apply("type2")), MergeReason.MAPPING_UPDATE, false);
        // adding new fields from different type is not ok
        String mapping2 = Strings.toString(
                XContentFactory.jsonBuilder().startObject().startObject("type3").startObject("properties").startObject("nested3")
                        .field("type", "nested").startObject("properties").endObject().endObject().endObject().endObject().endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
                mapperService.merge("type3", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, false));
        assertThat(e.getMessage(), containsString("Limit of nested fields [2] in index [test4] has been exceeded"));

        // do not check nested fields limit if mapping is not updated
        createIndex("test5", Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 0).build())
                .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_RECOVERY, false);
    }

}
