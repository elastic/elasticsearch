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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;

public class LegacyFieldAliasMapperTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testFieldAliasDisallowedWithMultipleTypes() throws IOException {
        Version version = VersionUtils.randomVersionBetween(random(), null, Version.V_5_6_0);
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, version)
                .build();
        IndexService indexService = createIndex("alias-test", settings);
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("alias-field")
                .field("type", "alias")
                .field("path", "concrete-field")
                .endObject()
                .startObject("concrete-field")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject());
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Cannot create a field alias [alias-field] for index [alias-test]. Field aliases" +
                " can only be specified on indexes that enforce a single mapping type.", e.getMessage());
    }

}
