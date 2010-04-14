package org.elasticsearch.groovy.test.client

import java.util.concurrent.CountDownLatch
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.groovy.node.GNode
import org.elasticsearch.groovy.node.GNodeBuilder
import static org.elasticsearch.client.Requests.*

/**
 * @author kimchy (shay.banon)
 */
class DifferentApiExecutionTests extends GroovyTestCase {

    def GNode node

    protected void setUp() {
        GNodeBuilder nodeBuilder = new GNodeBuilder()
        nodeBuilder.settings {
            node {
                local = true
            }
        }

        node = nodeBuilder.node
    }

    protected void tearDown() {
        node.close
    }

    void testSimpleOperations() {
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
        assertEquals "test", response.index
        assertEquals "type1", response.type
        assertEquals "1", response.id

        def refresh = node.client.admin.indices.refresh {}
        assertEquals 0, refresh.response.failedShards

        def getR = node.client.get {
            index "test"
            type "type1"
            id "1"
        }
        assertTrue getR.response.exists
        assertEquals "test", getR.response.index
        assertEquals "type1", getR.response.type
        assertEquals "1", getR.response.id
        assertEquals '{"test":"value","complex":{"value1":"value1","value2":"value2"}}', getR.response.sourceAsString()
        assertEquals "value", getR.response.source.test
        assertEquals "value1", getR.response.source.complex.value1

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
        assertEquals "test", response.index
        assertEquals "type1", response.type
        assertEquals "1", response.id

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
            assertEquals "test", responseX.index
            assertEquals "test", indexR.response.index
            assertEquals "type1", responseX.type
            assertEquals "type1", indexR.response.type
            assertEquals "1", responseX.id
            assertEquals "1", indexR.response.id
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
            assertEquals "test", indexR.response.index
            assertEquals "type1", indexR.response.type
            assertEquals "1", indexR.response.id
            latch.countDown()
        }
        latch.await()
    }
}

