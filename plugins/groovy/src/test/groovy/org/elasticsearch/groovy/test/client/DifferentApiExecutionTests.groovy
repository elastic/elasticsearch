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

import java.util.concurrent.CountDownLatch
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.groovy.node.GNode
import org.elasticsearch.groovy.node.GNodeBuilder
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import static org.elasticsearch.client.Requests.*
import static org.hamcrest.MatcherAssert.*
import static org.hamcrest.Matchers.*

/**
 * @author kimchy (shay.banon)
 */
class DifferentApiExecutionTests {

    def GNode node

    @BeforeMethod
    protected void setUp() {
        GNodeBuilder nodeBuilder = new GNodeBuilder()
        nodeBuilder.settings {
            node {
                local = true
            }
        }

        node = nodeBuilder.node()
    }

    @AfterMethod
    protected void tearDown() {
        node.close()
    }

    @Test
    void verifyDifferentApiExecutions() {
        def response = node.client.index(new IndexRequest(
                index: "test",
                type: "type1",
                id: "1",
                source: {
                    test = "value"
                    complex {
                        value1 = "value1"
                        value2 = "value2"
                    }
                })).response
        assertThat response.index, equalTo("test")
        assertThat response.type, equalTo("type1")
        assertThat response.id, equalTo("1")

        def refresh = node.client.admin.indices.refresh {}
        assertThat 0, equalTo(refresh.response.failedShards)

        def getR = node.client.get {
            index "test"
            type "type1"
            id "1"
        }
        assertThat getR.response.exists, equalTo(true)
        assertThat getR.response.index, equalTo("test")
        assertThat getR.response.type, equalTo("type1")
        assertThat getR.response.id, equalTo("1")
        assertThat getR.response.sourceAsString(), equalTo('{"test":"value","complex":{"value1":"value1","value2":"value2"}}')
        assertThat getR.response.source.test, equalTo("value")
        assertThat getR.response.source.complex.value1, equalTo("value1")

        response = node.client.index({
            index = "test"
            type = "type1"
            id = "1"
            source = {
                test = "value"
                complex {
                    value1 = "value1"
                    value2 = "value2"
                }
            }
        }).response
        assertThat response.index, equalTo("test")
        assertThat response.type, equalTo("type1")
        assertThat response.id, equalTo("1")

        def indexR = node.client.index(indexRequest().with {
            index "test"
            type "type1"
            id "1"
            source {
                test = "value"
                complex {
                    value1 = "value1"
                    value2 = "value2"
                }
            }
        })
        CountDownLatch latch = new CountDownLatch(1)
        indexR.success = {IndexResponse responseX ->
            assertThat responseX.index, equalTo("test")
            assertThat indexR.response.index, equalTo("test")
            assertThat responseX.type, equalTo("type1")
            assertThat indexR.response.type, equalTo("type1")
            assertThat response.id, equalTo("1")
            assertThat indexR.response.id, equalTo("1")
            latch.countDown()
        }
        latch.await()

        indexR = node.client.index {
            index "test"
            type "type1"
            id "1"
            source {
                test = "value"
                complex {
                    value1 = "value1"
                    value2 = "value2"
                }
            }
        }
        latch = new CountDownLatch(1)
        indexR.listener = {
            assertThat indexR.response.index, equalTo("test")
            assertThat indexR.response.type, equalTo("type1")
            assertThat indexR.response.id, equalTo("1")
            latch.countDown()
        }
        latch.await()
    }
}

