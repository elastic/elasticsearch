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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

public class DynamicMappingDisabledIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(MapperService.DYNAMIC_MAPPING_ENABLED_SETTING, "false")
            .build();
    }

    public void testDynamicDisabled() throws IOException {
        try {
            client().prepareIndex("index", "type", "1").setSource("foo", 3).get();
            fail("Indexing request should have failed");
        } catch (TypeMissingException e) {
            assertEquals(e.getMessage(), "type[[type, trying to auto create mapping for [type] in index [index], but dynamic mapping is disabled]] missing");
        }
    }
}
