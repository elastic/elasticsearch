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
        Version baseVersion = Version.fromString(props.getProperty("elasticsearch"));
        if (baseVersion.isSnapshot()) {
            throw new IllegalArgumentException("version.properties can't contain a snapshot version for elasticsearch. " +
                "Use the `build.snapshot` property instead.");
        }
        if (baseVersion.getQualifier().isEmpty() == false) {
            throw new IllegalArgumentException("version.properties can't contain a version qualifier for elasticsearch." +
                "Use the `build.version_qualifier` property instead.");
        }
        elasticsearch = new Version(
            baseVersion.getMajor(),
            baseVersion.getMinor(),
            baseVersion.getRevision(),
            // TODO: Change default to "" after CI "warm-up" by adding -Dbuild.version_qualifier="" to ML jobs to produce
            //       a snapshot.
            System.getProperty("build.version_qualifier", "alpha1"),
            Boolean.parseBoolean(System.getProperty("build.snapshot", "true"))
        );
        lucene = props.getProperty("lucene");
        for (String property : props.stringPropertyNames()) {
            versions.put(property, props.getProperty(property));
        }
        versions.put("elasticsearch", elasticsearch.toString());
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
