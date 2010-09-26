//@Grapes([
//    @Grab(group = 'org.elasticsearch', module = 'elasticsearch-groovy', version = '0.7.0-SNAPSHOT'),
//    @Grab(group = 'org.slf4j', module = 'slf4j-simple', version = '1.5.8')
///*    @Grab(group = 'org.slf4j', module = 'slf4j-log4j12', version = '1.5.8')*/
//])

def startNode() {
    def nodeBuilder = new org.elasticsearch.groovy.node.GNodeBuilder()
    nodeBuilder.settings {
        node {
            client = true
        }
    }
    nodeBuilder.node()
}


def node = startNode()

println "settings $node.settings.asMap"

println "Node started"

future = node.client.index {
    index "twitter"
    type "tweet"
    id "1"
    source {
        user = "kimchy"
        message = "this is a tweet"
    }
}

println "Indexed $future.response.index/$future.response.type/$future.response.id"

node.close()
