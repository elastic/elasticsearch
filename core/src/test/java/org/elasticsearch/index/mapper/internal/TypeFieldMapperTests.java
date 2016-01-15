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

package org.elasticsearch.index.mapper.internal;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class TypeFieldMapperTests extends ESSingleNodeTestCase {

    public void testDocValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        TypeFieldMapper typeMapper = docMapper.metadataMapper(TypeFieldMapper.class);
        assertTrue(typeMapper.fieldType().hasDocValues());
    }

    public void testDocValuesPre21() throws Exception {
        // between 2.0 and 2.1, doc values was disabled for _type
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        Settings bwcSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0_beta1.id).build();

        DocumentMapper docMapper = createIndex("test", bwcSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        TypeFieldMapper typeMapper = docMapper.metadataMapper(TypeFieldMapper.class);
        assertFalse(typeMapper.fieldType().hasDocValues());
    }
}
