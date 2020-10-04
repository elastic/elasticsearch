/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;

public class ICUCollationKeywordFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        ICUCollationKeywordFieldMapper mapper = new ICUCollationKeywordFieldMapper.Builder("field").build(context);
        assertEquals(List.of("42"), fetchSourceValue(mapper.fieldType(), 42L));
        assertEquals(List.of("true"), fetchSourceValue(mapper.fieldType(), true));

        ICUCollationKeywordFieldMapper ignoreAboveMapper = new ICUCollationKeywordFieldMapper.Builder("field")
            .ignoreAbove(4)
            .build(context);
        assertEquals(List.of(), fetchSourceValue(ignoreAboveMapper.fieldType(), "value"));
        assertEquals(List.of("42"), fetchSourceValue(ignoreAboveMapper.fieldType(), 42L));
        assertEquals(List.of("true"), fetchSourceValue(ignoreAboveMapper.fieldType(), true));

        ICUCollationKeywordFieldMapper nullValueMapper = new ICUCollationKeywordFieldMapper.Builder("field")
            .nullValue("NULL")
            .build(context);
        assertEquals(List.of("NULL"), fetchSourceValue(nullValueMapper.fieldType(), null));
    }
}
