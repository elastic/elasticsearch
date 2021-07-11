/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite;

import org.gradle.api.Project;
import org.gradle.api.plugins.quality.CodeQualityExtension;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class RewriteExtension extends CodeQualityExtension {
    private static final String magicalMetricsLogString = "LOG";

    private final List<String> activeRecipes = new ArrayList<>();
    private final List<String> activeStyles = new ArrayList<>();
    private boolean configFileSetDeliberately = false;
    private final Project project;
    private File configFile;
    private String metricsUri = magicalMetricsLogString;
    private String rewriteVersion = "7.8.1";
    private boolean logCompilationWarningsAndErrors;

    /**
     * Whether to throw an exception if an activeRecipe fails configuration validation.
     * This may happen if the activeRecipe is improperly configured, or any downstream recipes are improperly configured.
     * <p>
     * For the time, this default is "false" to prevent one improperly recipe from failing the build.
     * In the future, this default may be changed to "true" to be more restrictive.
     */
    private boolean failOnInvalidActiveRecipes = false;

    private boolean failOnDryRunResults = false;

    @SuppressWarnings("unused")
    public RewriteExtension(Project project) {
        this.project = project;
        configFile = project.getRootProject().file("rewrite.yml");
    }

    public void setConfigFile(File configFile) {
        configFileSetDeliberately = true;
        this.configFile = configFile;
    }

    public void setConfigFile(String configFilePath) {
        configFileSetDeliberately = true;
        configFile = project.file(configFilePath);
    }

    /**
     * Supplying a rewrite configuration file is optional, so if it doesn't exist it isn't an error or a warning.
     * But if the user has deliberately specified a different location from the default, that seems like a reasonable
     * signal that the file should be expected to exist. So this signal can be used to decide if a warning should be
     * displayed if the specified file cannot be found.
     */
    boolean getConfigFileSetDeliberately() {
        return configFileSetDeliberately;
    }

    public File getConfigFile() {
        return configFile;
    }

    public void enableRouteMetricsToLog() {
        metricsUri = magicalMetricsLogString;
    }

    public boolean isRouteMetricsToLog() {
        return metricsUri.equals(magicalMetricsLogString);
    }

    public String getMetricsUri() {
        return metricsUri;
    }

    public void setMetricsUri(String value) {
        metricsUri = value;
    }

    public void activeRecipe(String... recipes) {
        activeRecipes.addAll(asList(recipes));
    }

    public void clearActiveRecipes() {
        activeRecipes.clear();
    }

    public void setActiveRecipes(List<String> activeRecipes) {
        this.activeRecipes.clear();
        this.activeRecipes.addAll(activeRecipes);
    }

    public void activeStyle(String... styles) {
        activeStyles.addAll(asList(styles));
    }

    public void clearActiveStyles() {
        activeStyles.clear();
    }

    public void setActiveStyles(List<String> activeStyles) {
        this.activeRecipes.clear();
        this.activeRecipes.addAll(activeStyles);
    }

    public List<String> getActiveStyles() {
        return activeStyles;
    }

    public List<String> getActiveRecipes() {
        return activeRecipes;
    }

    public String getRewriteVersion() {
        return rewriteVersion;
    }

    public void setRewriteVersion(String value) {
        rewriteVersion = value;
    }

    public boolean getFailOnInvalidActiveRecipes() {
        return failOnInvalidActiveRecipes;
    }

    public void setFailOnInvalidActiveRecipes(boolean failOnInvalidActiveRecipes) {
        this.failOnInvalidActiveRecipes = failOnInvalidActiveRecipes;
    }

    public boolean getFailOnDryRunResults() {
        return this.failOnDryRunResults;
    }

    public void setFailOnDryRunResults(boolean failOnDryRunResults) {
        this.failOnDryRunResults = failOnDryRunResults;
    }

    public boolean getLogCompilationWarningsAndErrors() {
        return logCompilationWarningsAndErrors;
    }

    public void setLogCompilationWarningsAndErrors(boolean logCompilationWarningsAndErrors) {
        this.logCompilationWarningsAndErrors = logCompilationWarningsAndErrors;
    }
}
