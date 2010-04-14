package org.elasticsearch.groovy.test.node

import org.elasticsearch.groovy.node.GNode
import org.elasticsearch.groovy.node.GNodeBuilder
import static org.elasticsearch.groovy.node.GNodeBuilder.*

/**
 * @author kimchy (shay.banon)
 */
class GNodeBuilderTests extends GroovyTestCase {

    void testGNodeBuilder() {
        GNodeBuilder nodeBuilder = nodeBuilder();
        nodeBuilder.settings {
            node {
                local = true
            }
            cluster {
                name = "test"
            }
        }
        GNode node = nodeBuilder.node
        node.stop.close
    }
}
