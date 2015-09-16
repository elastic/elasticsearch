package org.elasticsearch.gradle

/**
 * Accessor for properties about the version of elasticsearch this was built with.
 */
class ElasticsearchProperties {
    static final String version
    static {
        Properties props = new Properties()
        props.load(ElasticsearchProperties.class.getResourceAsStream('/elasticsearch.properties'))
        version = props.getProperty('version')
    }
}
