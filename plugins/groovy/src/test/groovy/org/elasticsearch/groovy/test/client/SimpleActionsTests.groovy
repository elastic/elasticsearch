package org.elasticsearch.groovy.test.client

import org.elasticsearch.groovy.node.GNode
import org.elasticsearch.groovy.node.GNodeBuilder

/**
 * @author kimchy (shay.banon)
 */
class SimpleActionsTests extends GroovyTestCase {

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
        def indexR = node.client.index {
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
        assertEquals "test", indexR.response.index
        assertEquals "type1", indexR.response.type
        assertEquals "1", indexR.response.id

        def delete = node.client.delete {
            index "test"
            type "type1"
            id "1"
        }
        assertEquals "test", delete.response.index
        assertEquals "type1", delete.response.type
        assertEquals "1", delete.response.id

        def refresh = node.client.admin.indices.refresh {}
        assertEquals 0, refresh.response.failedShards

        def get = node.client.get {
            index "test"
            type "type1"
            id "1"
        }
        assertFalse get.response.exists

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
        assertEquals "test", indexR.response.index
        assertEquals "type1", indexR.response.type
        assertEquals "1", indexR.response.id

        refresh = node.client.admin.indices.refresh {}
        assertEquals 0, refresh.response.failedShards

        def count = node.client.count {
            indices "test"
            types "type1"
            query {
                term {
                    test = "value"
                }
            }
        }
        assertEquals 0, count.response.failedShards
        assertEquals 1, count.response.count

        def search = node.client.search {
            indices "test"
            types "type1"
            source {
                query {
                    term(test: "value")
                }
            }
        }
        assertEquals 0, search.response.failedShards
        assertEquals 1, search.response.hits.totalHits
        assertEquals "value", search.response.hits[0].source.test

        def deleteByQuery = node.client.deleteByQuery {
            indices "test"
            query {
                term("test": "value")
            }
        }
        assertEquals 0, deleteByQuery.response.indices.test.failedShards

        refresh = node.client.admin.indices.refresh {}
        assertEquals 0, refresh.response.failedShards

        get = node.client.get {
            index "test"
            type "type1"
            id "1"
        }
        assertFalse get.response.exists
    }
}
