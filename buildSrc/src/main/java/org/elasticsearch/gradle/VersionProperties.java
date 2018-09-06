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
    public static Version getElasticsearchVersion() {
        return elasticsearchVersion;
    }

    public static String getElasticsearch() {
        return elaticsearchBuild;
    }

    public static String getLucene() {
        return lucene;
    }

    public static Map<String, String> getVersions() {
        return versions;
    }

    private static final Version elasticsearchVersion;
    private static final String lucene;
    private static final String elaticsearchBuild;
    private static final Map<String, String> versions = new HashMap<String, String>();
    static {
        Properties props = getVersionProperties();
        lucene = props.getProperty("lucene");
        elasticsearchVersion = Version.fromString(props.getProperty("elasticsearch"));
        elaticsearchBuild = props.getProperty("elasticsearchBuild");
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
