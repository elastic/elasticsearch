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

package org.elasticsearch.groovy.test.client

import org.elasticsearch.groovy.node.GNode
import org.elasticsearch.groovy.node.GNodeBuilder
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import static org.hamcrest.MatcherAssert.*
import static org.hamcrest.Matchers.*

/**
 * @author kimchy (shay.banon)
 */

class BuilderActionsTests {

    def GNode node

    @BeforeMethod
    protected void setUp() {
        GNodeBuilder nodeBuilder = new GNodeBuilder()
        nodeBuilder.settings {
            node {
                local = true
            }
            gateway {
                type = "none"
            }
        }

        node = nodeBuilder.node()
    }

    @AfterMethod
    protected void tearDown() {
        node.close()
    }


    @Test
    void testSimpleOperations() {
        def indexR = node.client.prepareIndex("test", "type1", "1").setSource({
            test = "value"
            complex {
                value1 = "value1"
                value2 = "value2"
            }
        }).gexecute()

        assertThat indexR.response.index, equalTo("test")
        assertThat indexR.response.type, equalTo("type1")
        assertThat indexR.response.id, equalTo("1")

        node.client.admin.indices.refresh {}.actionGet()

        def countR = node.client.prepareCount("test").setQuery({
            term(test: "value")
        }).gexecute();

        assertThat countR.response.count, equalTo(1l)

        def searchR = node.client.prepareSearch("test").setQuery({
            term(test: "value")
        }).gexecute();

        assertThat searchR.response.hits.totalHits, equalTo(1l)

        def delete = node.client.prepareDelete("test", "type1", "1").gexecute()
        assertThat delete.response.index, equalTo("test")
        assertThat delete.response.type, equalTo("type1")
        assertThat delete.response.id, equalTo("1")

        def refresh = node.client.admin.indices.refresh {}
        assertThat refresh.response.failedShards, equalTo(0)

        def get = node.client.prepareGet("test", "type1", "1").gexecute()
        assertThat get.response.exists, equalTo(false)

    }
}