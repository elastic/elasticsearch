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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class RoutingTableTest extends ElasticsearchAllocationTestCase  {
    
    private static final String TEST_INDEX_1 = "test1";
    private static final String TEST_INDEX_2 = "test2";
    private RoutingTable emptyRoutingTable;
    private RoutingTable testRoutingTable;
    private int numberOfShards;
    private int numberOfReplicas;
    private final static Settings DEFAULT_SETTINGS = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
            
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.numberOfShards = randomIntBetween(1, 5);
        this.numberOfReplicas = randomIntBetween(1, 5);
        this.emptyRoutingTable = new RoutingTable.Builder().build();
        this.testRoutingTable = new RoutingTable.Builder()
            .add(new IndexRoutingTable.Builder(TEST_INDEX_1).initializeAsNew(createIndexMetaData(TEST_INDEX_1)).build())
            .add(new IndexRoutingTable.Builder(TEST_INDEX_2).initializeAsNew(createIndexMetaData(TEST_INDEX_2)).build())
            .build();
        System.out.println(this.numberOfShards + " shards; "+ this.numberOfReplicas + " replicas.");
    }
    
    private IndexMetaData createIndexMetaData(String indexName) {
        return new IndexMetaData.Builder(indexName)
        .settings(DEFAULT_SETTINGS)
        .numberOfReplicas(this.numberOfReplicas)
        .numberOfShards(this.numberOfShards)
        .build();
    }

    @Test
    public void testActivePrimaryShardsGrouped() {
        // empty routing table
        assertThat(this.emptyRoutingTable.activePrimaryShardsGrouped(new String[0], true).size(), is(0));
        assertThat(this.emptyRoutingTable.activePrimaryShardsGrouped(new String[0], false).size(), is(0));
        
        // routing table with two indices
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[] {TEST_INDEX_1}, false).size(), is(0));
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[] {TEST_INDEX_1}, true).size(), 
                is(this.numberOfShards));
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[] {TEST_INDEX_1, TEST_INDEX_2}, true).size(), 
                is(2 * this.numberOfShards));
        try {
            this.testRoutingTable.activePrimaryShardsGrouped(new String[] {TEST_INDEX_1, "not_exists"}, true);
            fail("Calling with non-existing index name should raise IndexMissingException");
        } catch (IndexMissingException e) {
            // expected
        }
    }
    
    @Test
    public void testAllActiveShardsGrouped() {
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[] {TEST_INDEX_1}, false).size(), is(0));
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[] {TEST_INDEX_1}, true).size(), 
                is(this.numberOfShards * (this.numberOfReplicas + 1)));
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[] {TEST_INDEX_1, TEST_INDEX_2}, true).size(), 
                is(2 * this.numberOfShards * (this.numberOfReplicas + 1)));
        try {
           this.testRoutingTable.allActiveShardsGrouped(new String[] {TEST_INDEX_1, "not_exists"}, true);
        } catch (IndexMissingException e) {
            fail("Calling with non-existing index should be ignored at the moment");
        }
    }
    
    @Test
    public void testAllAssignedShardsGrouped() {
        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[] {TEST_INDEX_1}, false).size(), is(0));
        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[] {TEST_INDEX_1}, true).size(), 
                is(this.numberOfShards * (this.numberOfReplicas + 1)));
        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[] {TEST_INDEX_1, TEST_INDEX_2}, true).size(), 
                is(2 * this.numberOfShards * (this.numberOfReplicas + 1)));
        try {
           this.testRoutingTable.allAssignedShardsGrouped(new String[] {TEST_INDEX_1, "not_exists"}, true);
        } catch (IndexMissingException e) {
            fail("Calling with non-existing index should be ignored at the moment");
        }
    }

}
