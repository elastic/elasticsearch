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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;

public class DeleteIndexTests extends ElasticsearchIntegrationTest {

    @Test
    public void testFilteredAlias() {
        assertAcked(prepareCreate("test1").addMapping("test", "field", "type=string"));
        createIndex("test2");
        assertAcked(client().admin().indices().prepareAliases()
                .addAlias("test1", "alias1", QueryBuilders.termQuery("field", "value"))
                .addAlias("test2", "alias2"));

        try {
            client().admin().indices().prepareDelete("alias1", "alias2").get();
            fail("delete index should have failed");
        } catch(UnsupportedOperationException e) {
            //all good
        }

        assertThat(client().admin().cluster().prepareState().get().getState().metaData().indices().size(), equalTo(2));

        assertAcked(client().admin().indices().prepareDelete("alias2"));
        assertThat(client().admin().cluster().prepareState().get().getState().metaData().indices().size(), equalTo(1));
    }
}
