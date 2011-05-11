/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author imotov
 */
public class FilteringAliasesTests {


    @Test public void testAliasFilters() throws Exception {


        MetaData metaData = new MetaData.Builder()
                .put(indexMetaData("index1", "alias1,alias2", "filter1,filter2"))
                .put(indexMetaData("index2"))
                .put(indexMetaData("index3"))
                .build();

        byte[][] filters = metaData.aliasFilters(new String[]{"alias1", "index2"}, "index1");
        assertThat(filters.length, equalTo(1));
        assertThat(filters[0], equalTo("filter1".getBytes()));

        // Index 1 is present unfiltered as well as filtered -> filter should have no effect
        filters = metaData.aliasFilters(new String[]{"alias1", "index1"}, "index1");
        assertThat(filters, nullValue());

    }

    private IndexMetaData indexMetaData(String name) {
        return indexMetaData(name, null, null);
    }

    private IndexMetaData indexMetaData(String name, String aliases, String filters) {
        IndexMetaData.Builder builder = new IndexMetaData.Builder(name);
        if (aliases != null || filters != null) {
            ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();

            if (aliases != null) {
                settings.putArray("index.aliases", aliases.split(","));
            }
            if (filters != null) {
                String[] filterArr = filters.split(",");
                for (int i = 0; i < filterArr.length; i++) {
                    filterArr[i] = Base64.encodeBytes(filterArr[i].getBytes());
                }
                settings.putArray("index.alias_filters", filterArr);
            }
            builder.settings(settings);
        }
        return builder.numberOfReplicas(1).numberOfShards(5).build();
    }
}
