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
class SimpleActionsTests {

    def GNode node

    @BeforeMethod
    protected void setUp() {
        GNodeBuilder nodeBuilder = new GNodeBuilder()
        nodeBuilder.settings {
            node {
                local = true
            }
        }

        node = nodeBuilder.node
    }

    @AfterMethod
    protected void tearDown() {
        node.close
    }


    @Test
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
        assertThat indexR.response.index, equalTo("test")
        assertThat indexR.response.type, equalTo("type1")
        assertThat indexR.response.id, equalTo("1")

        def delete = node.client.delete {
            index "test"
            type "type1"
            id "1"
        }
        assertThat delete.response.index, equalTo("test")
        assertThat delete.response.type, equalTo("type1")
        assertThat delete.response.id, equalTo("1")

        def refresh = node.client.admin.indices.refresh {}
        assertThat refresh.response.failedShards, equalTo(0)

        def get = node.client.get {
            index "test"
            type "type1"
            id "1"
        }
        assertThat get.response.exists, equalTo(false)

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
        assertThat indexR.response.index, equalTo("test")
        assertThat indexR.response.type, equalTo("type1")
        assertThat indexR.response.id, equalTo("1")

        refresh = node.client.admin.indices.refresh {}
        assertThat refresh.response.failedShards, equalTo(0)

        def count = node.client.count {
            indices "test"
            types "type1"
            query {
                term {
                    test = "value"
                }
            }
        }
        assertThat count.response.failedShards, equalTo(0)
        assertThat count.response.count, equalTo(1l)

        def search = node.client.search {
            indices "test"
            types "type1"
            source {
                query {
                    term(test: "value")
                }
            }
        }
        assertThat search.response.failedShards, equalTo(0)
        assertThat search.response.hits.totalHits, equalTo(1l)
        assertThat search.response.hits[0].source.test, equalTo("value")

        def deleteByQuery = node.client.deleteByQuery {
            indices "test"
            query {
                term("test": "value")
            }
        }
        assertThat deleteByQuery.response.indices.test.failedShards, equalTo(0)

        refresh = node.client.admin.indices.refresh {}
        assertThat refresh.response.failedShards, equalTo(0)

        get = node.client.get {
            index "test"
            type "type1"
            id "1"
        }
        assertThat get.response.exists, equalTo(false)
    }
}
