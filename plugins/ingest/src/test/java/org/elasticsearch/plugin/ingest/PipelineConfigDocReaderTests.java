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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class PipelineConfigDocReaderTests extends ESSingleNodeTestCase {

    public void testReadAll() {
        PipelineConfigDocReader reader = new PipelineConfigDocReader(Settings.EMPTY, node().injector());
        reader.start();

        createIndex(PipelineStore.INDEX);
        int numDocs = scaledRandomIntBetween(32, 128);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(PipelineStore.INDEX, PipelineStore.TYPE, Integer.toString(i))
                    .setSource("field", "value" + i)
                    .get();
        }
        client().admin().indices().prepareRefresh().get();

        int i = 0;
        for (SearchHit hit : reader.readAll()) {
            assertThat(hit.getId(), equalTo(Integer.toString(i)));
            assertThat(hit.getVersion(), equalTo(1l));
            assertThat(hit.getSource().get("field"), equalTo("value" + i));
            i++;
        }
        assertThat(i, equalTo(numDocs));
    }

}
