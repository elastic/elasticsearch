/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.checkstyle;

import com.google.common.util.concurrent.Callables;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.Directory;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFile;
import org.gradle.api.internal.ConventionMapping;
import org.gradle.api.plugins.quality.CheckstyleExtension;
import org.gradle.api.plugins.quality.CodeQualityExtension;
import org.gradle.api.plugins.quality.internal.AbstractCodeQualityPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.resources.TextResource;
import org.gradle.api.tasks.SourceSet;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.gradle.api.internal.lambdas.SerializableLambdas.action;

/**
 * Checkstyle Plugin.
 *
 * @see <a href="https://docs.gradle.org/current/userguide/checkstyle_plugin.html">Checkstyle plugin reference</a>
 */
public class CheckstylePlugin extends AbstractCodeQualityPlugin<CheckstyleTask> {

    public static final String DEFAULT_CHECKSTYLE_VERSION = "8.37";
    private static final String CONFIG_DIR_NAME = "config/checkstyle";
    private CheckstyleExtension extension;

    @Override
    protected String getToolName() {
        return "Checkstyle";
    }

    @Override
    protected Class<CheckstyleTask> getTaskType() {
        return CheckstyleTask.class;
    }

    @Override
    protected CodeQualityExtension createExtension() {

//         extension = project.getExtensions().findByType(CheckstyleExtension.class);
        extension = project.getExtensions().create("checkstyle", CheckstyleExtension.class, project);

         extension.setToolVersion(DEFAULT_CHECKSTYLE_VERSION);
         Directory directory = project.getRootProject().getLayout().getProjectDirectory().dir(CONFIG_DIR_NAME);
         extension.getConfigDirectory().convention(directory);
         extension.setConfig(project.getResources().getText().fromFile(extension.getConfigDirectory().file("checkstyle.xml")
         // If for whatever reason the provider above cannot be resolved, go back to default location, which we know how to ignore if
         // missing
         .orElse(directory.file("checkstyle.xml"))));
         return extension;
//        return extension;
    }

    @Override
    protected void configureConfiguration(Configuration configuration) {
        configureDefaultDependencies(configuration);
    }

    @Override
    protected void configureTaskDefaults(CheckstyleTask task, final String baseName) {
        Configuration configuration = project.getConfigurations().getAt(getConfigurationName());
        configureTaskConventionMapping(configuration, task);
        configureReportsConventionMapping(task, baseName);
    }

    private void configureDefaultDependencies(Configuration configuration) {
        configuration.defaultDependencies(
            dependencies -> dependencies.add(
                project.getDependencies().create("com.puppycrawl.tools:checkstyle:" + extension.getToolVersion())
            )
        );
    }

    private void configureTaskConventionMapping(Configuration configuration, CheckstyleTask task) {
        ConventionMapping taskMapping = task.getConventionMapping();
        taskMapping.map("checkstyleClasspath", Callables.returning(configuration));
        taskMapping.map("config", (Callable<TextResource>) () -> extension.getConfig());
        taskMapping.map("configProperties", (Callable<Map<String, Object>>) () -> extension.getConfigProperties());
        taskMapping.map("ignoreFailures", (Callable<Boolean>) () -> extension.isIgnoreFailures());
        taskMapping.map("showViolations", (Callable<Boolean>) () -> extension.isShowViolations());
        taskMapping.map("maxErrors", (Callable<Integer>) () -> extension.getMaxErrors());
        taskMapping.map("maxWarnings", (Callable<Integer>) () -> extension.getMaxWarnings());

        task.getConfigDirectory().convention(extension.getConfigDirectory());
    }

    private void configureReportsConventionMapping(CheckstyleTask task, final String baseName) {
        ProjectLayout layout = project.getLayout();
        ProviderFactory providers = project.getProviders();
        Provider<RegularFile> reportsDir = layout.file(providers.provider(() -> extension.getReportsDir()));
        task.getReports().all(action(report -> {
            report.getRequired().convention(true);
            report.getOutputLocation().convention(layout.getProjectDirectory().file(providers.provider(() -> {
                String reportFileName = baseName + "." + report.getName();
                return new File(reportsDir.get().getAsFile(), reportFileName).getAbsolutePath();
            })));
        }));
    }

    @Override
    protected void configureForSourceSet(final SourceSet sourceSet, CheckstyleTask task) {
        task.setDescription("Run Checkstyle analysis for " + sourceSet.getName() + " classes");
        task.setClasspath(sourceSet.getOutput().plus(sourceSet.getCompileClasspath()));
        task.setSource(sourceSet.getAllJava());
    }
}
