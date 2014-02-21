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

package org.elasticsearch.indices.mapping;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 */
@LuceneTestCase.Slow
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numNodes = 0)
public class DedicatedMasterGetFieldMappingTests extends SimpleGetFieldMappingsTests {

    @Before
    public void before1() {
        Settings settings = settingsBuilder()
                .put("node.data", false)
                .build();
        cluster().startNode(settings);
        cluster().startNode(ImmutableSettings.EMPTY);
    }

}
