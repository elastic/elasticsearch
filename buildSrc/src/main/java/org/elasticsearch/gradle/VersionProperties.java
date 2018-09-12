package org.elasticsearch.gradle;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Accessor for shared dependency versions used by elasticsearch, namely the elasticsearch and lucene versions.
 */
public class VersionProperties {
    public static Version getElasticsearch() {
        return elasticsearch;
    }

    public static String getLucene() {
        return lucene;
    }

    public static Map<String, String> getVersions() {
        return versions;
    }

    private static final Version elasticsearch;
    private static final String lucene;
    private static final Map<String, String> versions = new HashMap<String, String>();
    static {
        Properties props = getVersionProperties();
        elasticsearch = Version.fromString(props.getProperty("elasticsearch"));
        lucene = props.getProperty("lucene");
        for (String property : props.stringPropertyNames()) {
            versions.put(property, props.getProperty(property));
        }
    }

    private static Properties getVersionProperties() {
        Properties props = new Properties();
        InputStream propsStream = VersionProperties.class.getResourceAsStream("/version.properties");
        if (propsStream == null) {
            throw new RuntimeException("/version.properties resource missing");
        }
        try {
            props.load(propsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return props;
    }
}
