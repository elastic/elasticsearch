package org.elasticsearch.gradle

/**
 * Accessor for properties about the version of elasticsearch this was built with.
 */
class ElasticsearchProperties {
    static final String version
    static {
        Properties props = new Properties()
        InputStream propsStream = ElasticsearchProperties.class.getResourceAsStream('/elasticsearch.properties')
        if (propsStream == null) {
            throw new RuntimeException('/elasticsearch.properties resource missing')
        }
        props.load(propsStream)
        version = props.getProperty('version')
    }
}
