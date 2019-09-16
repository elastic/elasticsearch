package org.elasticsearch.gradle.plugin;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;

import java.util.stream.Collectors;

public class PluginBuildPluginTests extends GradleUnitTestCase {

    private Project project;

    @Before
    public void setUp() throws Exception {
        project = ProjectBuilder.builder()
            .withName(getClass().getName())
            .build();
    }

    public void testApply() {
        project.getPlugins().apply(PluginBuildPlugin.class);

        assertNotNull(
            "plugin extension created with the right name",
            project.getExtensions().findByName(PluginBuildPlugin.PLUGIN_EXTENSION_NAME)
        );
        assertNotNull(
            "plugin extensions has the right type",
            project.getExtensions().findByType(PluginPropertiesExtension.class)
        );

        assertNotNull(
            "plugin created an integTest class",
            project.getTasks().findByName("integTest")
        );
    }

    public void testApplyWithAfterEvaluate() {
        project.getPlugins().apply(PluginBuildPlugin.class);
        PluginPropertiesExtension extension = project.getExtensions().getByType(PluginPropertiesExtension.class);
        extension.setNoticeFile(project.file("test.notice"));
        extension.setDescription("just a test");
        extension.setClassname(getClass().getName());

        PluginBuildPlugin.configureAfterEvaluate(project);

        assertNotNull(
            "Task to generate notice not created: " + project.getTasks().stream()
                .map(Task::getPath)
                .collect(Collectors.joining(", ")),
            project.getTasks().findByName("generateNotice")
        );
    }
}
