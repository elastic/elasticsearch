/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions;

import org.apache.tools.ant.taskdefs.condition.Os;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Transformer;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Delete;
import org.gradle.plugins.ide.eclipse.EclipsePlugin;
import org.gradle.plugins.ide.eclipse.model.Classpath;
import org.gradle.plugins.ide.eclipse.model.EclipseModel;
import org.gradle.plugins.ide.eclipse.model.EclipseProject;
import org.gradle.plugins.ide.eclipse.model.ProjectDependency;
import org.gradle.plugins.ide.eclipse.model.SourceFolder;
import org.gradle.plugins.ide.eclipse.model.ClasspathEntry;

import java.io.File;
import java.util.List;
import java.io.IOException;
import static java.util.stream.Collectors.toList;

import static org.apache.commons.io.FileUtils.readFileToString;

public class EclipseConventionPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPlugins().apply(EclipsePlugin.class);
        EclipseModel eclipseModel = project.getExtensions().getByType(EclipseModel.class);
        EclipseProject eclipseProject = eclipseModel.getProject();

        // Name all the non-root projects after their path so that paths get grouped together when imported into eclipse.
        if (project.getPath().equals(":") == false) {
            eclipseProject.setName(project.getPath());
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                eclipseProject.setName(eclipseProject.getName().replace(':', '_'));
            }
        }


        File licenseHeaderFile;
        String prefix = ":x-pack";
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            prefix = prefix.replace(':', '_');
        }
        File root = root(project);
        if (eclipseProject.getName().startsWith(prefix)) {
            licenseHeaderFile = new File(root, "build-tools-internal/src/main/resources/license-headers/elastic-license-2.0-header.txt");
        } else {
            licenseHeaderFile = new File(root, "build-tools-internal/src/main/resources/license-headers/sspl+elastic-license-header.txt");
        }

        String lineSeparator = Os.isFamily(Os.FAMILY_WINDOWS) ? "\\\\r\\\\n" : "\\\\n";
        String licenseHeader = null;
        try {
            licenseHeader = readFileToString(licenseHeaderFile, "UTF-8").replace(System.lineSeparator(), lineSeparator);
        } catch (IOException e) {
            throw new GradleException("Cannot configure eclipse", e);
        }

        String finalLicenseHeader = licenseHeader;
        project.getTasks().register("copyEclipseSettings", Copy.class, copy -> {
                copy.mustRunAfter("wipeEclipseSettings");
                // TODO: "package this up" for external builds
                copy.from(new File(root, "build-tools-internal/src/main/resources/eclipse.settings"));
                copy.into(".settings");
                copy.filter(new Transformer<String, String>() {
                    @Override
                    public String transform(String s) {
                        return s.replaceAll("@@LICENSE_HEADER_TEXT@@", finalLicenseHeader);
                    }
                });
        });
        // otherwise .settings is not nuked entirely
        project.getTasks().register("wipeEclipseSettings", Delete.class, new Action<Delete>() {
            @Override
            public void execute(Delete delete) {
                delete.delete(".settings");
            }
        });

        project.getTasks().named("cleanEclipse").configure(t -> t.dependsOn("wipeEclipseSettings"));

        // otherwise the eclipse merging is *super confusing*
        project.getTasks().named("eclipse").configure(t -> t.dependsOn("cleanEclipse", "copyEclipseSettings"));

        project.getPlugins().withType(JavaBasePlugin.class, javaBasePlugin -> {
                JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);
                java.getModularity().getInferModulePath().set(false);

                eclipseModel.getClasspath().getFile().whenMerged(c -> {
                    Classpath classpath = (Classpath) c;

                    /*
                     * give each source folder a unique corresponding output folder
                     * outside of the usual `build` folder. We can't put the build
                     * in the usual build folder because eclipse becomes *very* sad
                     * if we delete it. Which `gradlew clean` does all the time.
                     */
                    int i = 0;
                    List<ClasspathEntry> sourceFolderList = classpath.getEntries().stream().filter(e -> e instanceof SourceFolder).collect(toList());
                    for (ClasspathEntry sourceFolder : sourceFolderList) {
                        ((SourceFolder)sourceFolder).setOutput("out/eclipse/" + i++);
                    }

                    // Starting with Gradle 6.7 test dependencies are not exposed by eclipse
                    // projects by default. This breaks project dependencies using the `java-test-fixtures` plugin
                    // or dependencies on other projects manually declared testArtifacts configurations
                    // This issue is tracked in gradle issue tracker.
                    // See https://github.com/gradle/gradle/issues/14932 for further details

                    classpath.getEntries().stream().filter(e -> e instanceof ProjectDependency).forEach(it ->
                            ((ProjectDependency) it).getEntryAttributes().remove("without_test_code")
                    );

                });

                project.getTasks().named("eclipseJdt").configure(t -> t.dependsOn("copyEclipseSettings"));
        });
    }

    private File root(Project project) {
        return project.getRootProject().getName().equals("elasticsearch") ?
                project.getRootProject().getRootDir() :
                project.getRootDir().getParentFile();
    }
}
