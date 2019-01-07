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
package org.elasticsearch.indices.exists.types;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;

public class TypesExistsIT extends ESIntegTestCase {

    public void testTypesExistsWithBlocks() throws IOException {
        assertAcked(prepareCreate("ro").addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject()));
        ensureGreen("ro");

        // Request is not blocked
        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("ro", block);
                assertThat(client().admin().indices().prepareTypesExists("ro").setTypes("type1").execute().actionGet().isExists(),
                    equalTo(true));
            } finally {
                disableIndexBlock("ro", block);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareTypesExists("ro").setTypes("type1"));
        } finally {
            disableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
        }
    }
}
