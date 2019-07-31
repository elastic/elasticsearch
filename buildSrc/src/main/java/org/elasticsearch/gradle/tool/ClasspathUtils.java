package org.elasticsearch.gradle.tool;

public class ClasspathUtils {
    private static boolean isElasticsearchProject;

    static {
        // look for buildSrc marker file, if it exists then we are running in the context of the elastic/elasticsearch build
        isElasticsearchProject = ClasspathUtils.class.getResource("/buildSrc.marker") != null;
    }

    private ClasspathUtils() {
    }

    /**
     * Determine if we are running in the context of the `elastic/elasticsearch` project. This method will return {@code false} when
     * the build-tools project is pulled in as an external dependency.
     *
     * @return if we are currently running in the `elastic/elasticsearch` project
     */
    public static boolean isElasticsearchProject() {
        return isElasticsearchProject;
    }
}
