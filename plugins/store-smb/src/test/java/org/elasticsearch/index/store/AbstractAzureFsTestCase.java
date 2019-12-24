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

package org.elasticsearch.index.store;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugin.store.smb.SMBStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

public abstract  class AbstractAzureFsTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SMBStorePlugin.class);
    }

    public void testAzureFs() {
        // Create an index and index some documents
        createIndex("test");
        long nbDocs = randomIntBetween(10, 1000);
        for (long i = 0; i < nbDocs; i++) {
            indexDoc("test", "" + i, "foo", "bar");
        }
        refresh();
        SearchResponse response = client().prepareSearch("test").get();
        assertThat(response.getHits().getTotalHits().value, is(nbDocs));
    }
}
