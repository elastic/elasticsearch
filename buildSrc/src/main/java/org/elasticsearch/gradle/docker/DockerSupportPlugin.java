package org.elasticsearch.gradle.docker;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Plugin providing {@link DockerSupportService} for detecting Docker installations and determining requirements for Docker-based
 * Elasticsearch build tasks.
 * <p>
 * Additionally registers a task graph listener used to assert a compatible Docker installation exists when task requiring Docker are
 * scheduled for execution. Tasks may declare a Docker requirement via an extra property. If a compatible Docker installation is not
 * available on the build system an exception will be thrown prior to task execution.
 *
 * <pre>
 *     task myDockerTask {
 *         ext.requiresDocker = true
 *     }
 * </pre>
 */
public class DockerSupportPlugin implements Plugin<Project> {
    public static final String DOCKER_SUPPORT_SERVICE_NAME = "dockerSupportService";
    public static final String DOCKER_ON_LINUX_EXCLUSIONS_FILE = ".ci/dockerOnLinuxExclusions";
    public static final String REQUIRES_DOCKER_ATTRIBUTE = "requiresDocker";

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }

        Provider<DockerSupportService> dockerSupportServiceProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(
                DOCKER_SUPPORT_SERVICE_NAME,
                DockerSupportService.class,
                spec -> spec.parameters(
                    params -> { params.setExclusionsFile(new File(project.getRootDir(), DOCKER_ON_LINUX_EXCLUSIONS_FILE)); }
                )
            );

        // Ensure that if any tasks declare they require docker, we assert an available Docker installation exists
        project.getGradle().getTaskGraph().whenReady(graph -> {
            List<String> dockerTasks = graph.getAllTasks().stream().filter(task -> {
                ExtraPropertiesExtension ext = task.getExtensions().getExtraProperties();
                return ext.has(REQUIRES_DOCKER_ATTRIBUTE) && (boolean) ext.get(REQUIRES_DOCKER_ATTRIBUTE);
            }).map(Task::getPath).collect(Collectors.toList());

            if (dockerTasks.isEmpty() == false) {
                dockerSupportServiceProvider.get().failIfDockerUnavailable(dockerTasks);
            }
        });
    }

}
