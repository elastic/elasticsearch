package org.elasticsearch.gradle.tool;

import org.gradle.api.Project;
import org.gradle.api.plugins.ExtraPropertiesExtension;

public class ClasspathUtils {

    private ClasspathUtils() {}

    /**
     * Determine if we are running in the context of the `elastic/elasticsearch` project. This method will return {@code false} when
     * the build-tools project is pulled in as an external dependency.
     *
     * @return if we are currently running in the `elastic/elasticsearch` project
     */
    public static boolean isElasticsearchProject(Project project) {
        ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        return (boolean) ext.get("isInternal");
    }
}
