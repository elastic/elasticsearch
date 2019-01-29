package org.elasticsearch.gradle;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Accessor for shared dependency versions used by elasticsearch, namely the elasticsearch and lucene versions.
 */
public class VersionProperties {
    private static final Pattern JDK_VERSION = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)\\+(\\d+)");

    public static String getElasticsearch() {
        return elasticsearch;
    }

    public static String getLucene() {
        return lucene;
    }

    /**
     * Return the bundled jdk version, broken into elements as:
     * [feature, interim, update, build]
     *
     * Note the "patch" version is not yet handled here, as it has not yet
     * been used by java.
     */
    public static List<String> getBundledJdk() {
        return bundledJdk;
    }

    public static Map<String, String> getVersions() {
        return versions;
    }

    private static final String elasticsearch;
    private static final String lucene;
    private static final List<String> bundledJdk;
    private static final Map<String, String> versions = new HashMap<String, String>();
    static {
        Properties props = getVersionProperties();
        elasticsearch = props.getProperty("elasticsearch");
        lucene = props.getProperty("lucene");

        String bundledJdkProperty = props.getProperty("bundled_jdk");
        Matcher jdkVersionMatcher = JDK_VERSION.matcher(bundledJdkProperty);
        if (jdkVersionMatcher.matches() == false) {
            throw new IllegalArgumentException("Malformed jdk version [" + bundledJdkProperty + "]");
        }
        List<String> bundledJdkList = new ArrayList<>();
        for (int i = 1; i <= jdkVersionMatcher.groupCount(); ++i) {
            bundledJdkList.add(jdkVersionMatcher.group(i));
        }
        bundledJdk = Collections.unmodifiableList(bundledJdkList);

        for (String property : props.stringPropertyNames()) {
            versions.put(property, props.getProperty(property));
        }
    }

    private static Properties getVersionProperties() {
        Properties props = new Properties();
        InputStream propsStream = VersionProperties.class.getResourceAsStream("/version.properties");
        if (propsStream == null) {
            throw new IllegalStateException("/version.properties resource missing");
        }
        try {
            props.load(propsStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load version properties", e);
        }
        return props;
    }

    public static boolean isElasticsearchSnapshot() {
        return elasticsearch.endsWith("-SNAPSHOT");
    }
}
