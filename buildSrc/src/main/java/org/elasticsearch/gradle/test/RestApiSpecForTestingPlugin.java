package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.tool.Boilerplate;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.SourceSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

//TODO: change logger.info to logger.debug
public class RestApiSpecForTestingPlugin implements Plugin<Project> {

    private static final Logger logger = Logging.getLogger(RestApiSpecForTestingPlugin.class);
    private static final String EXTENSION_NAME = "restApiSpec";
    private static final String apiDir = "rest-api-spec/api";
    private static final String testDir = "rest-api-spec/test";

    @Override
    public void apply(Project project) {

        RestApiSpecExtension extension = project.getExtensions().create(EXTENSION_NAME, RestApiSpecExtension.class);
        // need to defer to after evaluation to allow the custom extension to be populated
        project.afterEvaluate(p -> {
            try {
                // copy tests
                boolean hasCopiedTests = false;
                if (extension.shouldCopyCoreTests()) {
                    Configuration coreTestConfig = project.getConfigurations().create("copyRestSpecCoreTests");
                    Dependency coreTestDep = project.getDependencies()
                        .project(Map.of("path", ":rest-api-spec", "configuration", "restSpecCoreTests"));
                    logger.info("Rest specs tests for project [{}] will be copied to the test resources.", project.getPath());
                    createCopyTask(project, coreTestConfig, coreTestDep, extension.getIncludesCoreTests(), testDir, false);
                    hasCopiedTests = true;
                }
                if (extension.shouldCopyXpackTests()) {
                    Configuration xpackSpecTestsConfig = project.getConfigurations().create("copyRestSpecXpackTests");
                    Dependency xpackTestsDep = project.getDependencies()
                        .project(Map.of("path", ":x-pack:plugin", "configuration", "restSpecXpackTests"));
                    logger.info("Rest specs x-pack tests for project [{}] will be copied to the test resources.", project.getPath());
                    createCopyTask(project, xpackSpecTestsConfig, xpackTestsDep, extension.getIncludesXpackTests(), testDir, false);
                    hasCopiedTests = true;
                }
                // copy specs
                if (projectHasRestTests(project) || hasCopiedTests || extension.shouldAlwaysCopySpec()) {
                    Configuration coreSpecConfig = project.getConfigurations().create("restSpec"); // name chosen for passivity
                    Dependency coreSpecDep = project.getDependencies()
                        .project(Map.of("path", ":rest-api-spec", "configuration", "restSpecCore"));

                    if (BuildParams.isInternal()) {
                        // source the specs from this project - this is the path for Elasticsearch builds
                        if (project.getPath().startsWith(":x-pack")) {
                            Configuration xpackSpecConfig = project.getConfigurations().create("copyRestSpecXpack");
                            Dependency xpackSpecDep = project.getDependencies()
                                .project(Map.of("path", ":x-pack:plugin", "configuration", "restSpecXpack"));
                            logger.info("X-pack rest specs for project [{}] will be copied to the test resources.", project.getPath());
                            createCopyTask(project, xpackSpecConfig, xpackSpecDep, extension.getIncludesXpackSpec(), apiDir, false);
                        }

                        logger.info("Rest specs for project [{}] will be copied to the test resources.", project.getPath());
                        createCopyTask(project, coreSpecConfig, coreSpecDep, extension.getIncludesCoreSpec(), apiDir, false);

                    } else {
                        // source the specs from the published jar - this is the path plugin developers
                        logger.info(
                            "Rest specs for project [{}] will be copied to the test resources from the published jar (version: [{}]).",
                            project.getPath(),
                            VersionProperties.getElasticsearch()
                        );
                        Dependency coreSpecFromJarDep = project.getDependencies()
                            .create("org.elasticsearch:rest-api-spec:" + VersionProperties.getElasticsearch());

                        createCopyTask(project, coreSpecConfig, coreSpecFromJarDep, extension.getIncludesCoreSpec(), "", true);
                    }
                } else {
                    logger.info("Rest specs will be ignored for project [{}] since there are no REST tests", project.getPath());
                    return;
                }
            } catch (Exception e) {
                throw new IllegalStateException("Error configuring the rest-api-spec-for-testing plugin. This is likely a bug.", e);
            }
        });
    }

    private void createCopyTask(Project project, Configuration config, Dependency dep, List<String> includes, String into, boolean isJar) {
        project.getDependencies().add(config.getName(), dep);
        Copy copyTask = project.getTasks().create(config.getName() + "Task", Copy.class, copy -> {
            copy.from(isJar ? project.zipTree(config.getSingleFile()) : config.getSingleFile());
            copy.into(new File(getTestSourceSet(project).getOutput().getResourcesDir(), into));
            if (includes.isEmpty()) {
                copy.include(isJar ? apiDir + "/**" : "*/**");
            } else {
                includes.forEach(s -> copy.include(isJar ? apiDir + "/" + s + "*/**" : s + "*/**"));
            }
            copy.setIncludeEmptyDirs(false);
        });
        copyTask.dependsOn(config);
        project.getTasks().getByName("processTestResources").dependsOn(copyTask);
    }

    private boolean projectHasRestTests(Project project) throws IOException {
        File testResourceDir = getTestResourceDir(project);
        if (testResourceDir == null || new File(testResourceDir, "rest-api-spec/test").exists() == false) {
            return false;
        }
        return Files.walk(testResourceDir.toPath().resolve("rest-api-spec/test")).anyMatch(p -> p.getFileName().toString().endsWith("yml"));
    }

    private File getTestResourceDir(Project project) {
        SourceSet testSources = getTestSourceSet(project);
        if (testSources == null) {
            return null;
        }
        Set<File> resourceDir = testSources.getResources()
            .getSrcDirs()
            .stream()
            .filter(f -> f.isDirectory() && f.getParentFile().getName().equals("test") && f.getName().equals("resources"))
            .collect(Collectors.toSet());
        assert resourceDir.size() <= 1;
        if (resourceDir.size() == 0) {
            return null;
        }
        return resourceDir.iterator().next();
    }

    private SourceSet getTestSourceSet(Project project) {
        return Boilerplate.getJavaSourceSets(project).findByName("test");
    }
}
