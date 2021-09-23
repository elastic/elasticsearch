/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.workers.WorkAction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import java.util.Properties;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public abstract class RewriteWorker implements WorkAction<RewriteParameters> {

    private static Logger logger = Logging.getLogger(RewriteWorker.class);

    RewriteReflectiveFacade rewrite;
    @Override
    public void execute() {
        rewrite = new RewriteReflectiveFacade();
        ResultsContainer results = listResults();
        writeInPlaceChanges(results);
    }

    private void writeInPlaceChanges(ResultsContainer results) {
        for (RewriteReflectiveFacade.Result result : results.refactoredInPlace) {
            assert result.getBefore() != null;
            try (BufferedWriter sourceFileWriter = Files.newBufferedWriter(
                    results.getProjectRoot().resolve(result.getBefore().getSourcePath()))) {
                assert result.getAfter() != null;
                sourceFileWriter.write(result.getAfter().print());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected RewriteWorker.ResultsContainer listResults() {
        Path baseDir = getParameters().getProjectDirectory().get().getAsFile().toPath();
        RewriteReflectiveFacade.Environment env = environment();
        List<String> activeRecipes = getParameters().getActiveRecipes().get();
        List<String> activeStyles = getParameters().getActiveStyles().get();
        logger.lifecycle(String.format("Using active recipe(s) %s", activeRecipes));
        logger.lifecycle(String.format("Using active styles(s) %s", activeStyles));
        if (activeRecipes.isEmpty()) {
            return new RewriteWorker.ResultsContainer(baseDir, emptyList());
        }
        List<RewriteReflectiveFacade.NamedStyles> styles = env.activateStyles(activeStyles);
        RewriteReflectiveFacade.Recipe recipe = env.activateRecipes(activeRecipes);
        logger.lifecycle("Validating active recipes");
        Collection<RewriteReflectiveFacade.Validated> validated = recipe.validateAll();
        List<RewriteReflectiveFacade.Validated.Invalid> failedValidations = validated.stream()
                .map(RewriteReflectiveFacade.Validated::failures)
                .flatMap(Collection::stream)
                .collect(toList());
        if (failedValidations.isEmpty() == false) {
            failedValidations.forEach(
                    failedValidation -> logger.error(
                            "Recipe validation error in " + failedValidation.getProperty() + ": " + failedValidation.getMessage(),
                            failedValidation.getException()
                    )
            );
            logger.error(
                    "Recipe validation errors detected as part of one or more activeRecipe(s). Execution will continue regardless."
            );
        }

        RewriteReflectiveFacade.InMemoryExecutionContext ctx = executionContext();
        List<RewriteReflectiveFacade.SourceFile> sourceFiles = parse(
                getParameters().getAllJavaPaths().get(),
                getParameters().getAllDependencyPaths().get(),
                styles,
                ctx);

        logger.lifecycle("Running recipe(s)...");
        List<RewriteReflectiveFacade.Result> results = recipe.run(sourceFiles);
        return new RewriteWorker.ResultsContainer(baseDir, results);
    }

    protected RewriteReflectiveFacade.InMemoryExecutionContext executionContext() {
        return rewrite.inMemoryExecutionContext(t -> logger.warn(t.getMessage(), t));
    }

    protected List<RewriteReflectiveFacade.SourceFile> parse(
            List<File> javaFiles,
            List<File> dependencyFiles,
            List<RewriteReflectiveFacade.NamedStyles> styles,
            RewriteReflectiveFacade.InMemoryExecutionContext ctx
    ) {
        try {
            List<Path> javaPaths = javaFiles.stream().map(File::toPath).map(p -> p.toAbsolutePath()).toList();
            List<Path> dependencyPaths = dependencyFiles.stream().map(File::toPath).map(p -> p.toAbsolutePath()).toList();
            List<RewriteReflectiveFacade.SourceFile> sourceFiles = new ArrayList<>();
            if (javaPaths.size() > 0) {
                logger.lifecycle(
                        "Parsing " + javaPaths.size() + " Java files from "  + getParameters().getProjectDirectory().getAsFile().get()
                );
                Instant start = Instant.now();
                sourceFiles.addAll(
                        rewrite.javaParserFromJavaVersion()
                                .relaxedClassTypeMatching(true)
                                .styles(styles)
                                .classpath(dependencyPaths)
                                .charset(Charset.forName("UTF-8"))
                                .logCompilationWarningsAndErrors(false)
                                .build()
                                .parse(javaPaths, getParameters().getProjectDirectory().get().getAsFile().toPath(), ctx)
                );
            }
            return sourceFiles;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected RewriteReflectiveFacade.Environment environment() {
        Properties properties = new Properties();
        RewriteReflectiveFacade.EnvironmentBuilder env =
                rewrite.environmentBuilder(properties).scanRuntimeClasspath().scanUserHome();
        File rewriteConfig = getParameters().getConfigFile().getAsFile().getOrNull();
        if(rewriteConfig != null){
            if (rewriteConfig.exists()) {
                try (FileInputStream is = new FileInputStream(rewriteConfig)) {
                    RewriteReflectiveFacade.YamlResourceLoader resourceLoader =
                            rewrite.yamlResourceLoader(is, rewriteConfig.toURI(), properties);
                    env.load(resourceLoader);
                } catch (IOException e) {
                    throw new RuntimeException("Unable to load rewrite configuration", e);
                }
            } else {
                logger.warn("Rewrite configuration file " + rewriteConfig + " does not exist.");
            }
        }
        return env.build();
    }

    public static class ResultsContainer {
        final Path projectRoot;
        final List<RewriteReflectiveFacade.Result> generated = new ArrayList<>();
        final List<RewriteReflectiveFacade.Result> deleted = new ArrayList<>();
        final List<RewriteReflectiveFacade.Result> moved = new ArrayList<>();
        final List<RewriteReflectiveFacade.Result> refactoredInPlace = new ArrayList<>();

        public ResultsContainer(Path projectRoot, Collection<RewriteReflectiveFacade.Result> results) {
            this.projectRoot = projectRoot;
            for (RewriteReflectiveFacade.Result result : results) {
                if (result.getBefore() == null && result.getAfter() == null) {
                    // This situation shouldn't happen / makes no sense, log and skip
                    continue;
                }
                if (result.getBefore() == null && result.getAfter() != null) {
                    generated.add(result);
                } else if (result.getBefore() != null && result.getAfter() == null) {
                    deleted.add(result);
                } else if (result.getBefore() != null
                        && result.getBefore().getSourcePath().equals(result.getAfter().getSourcePath()) == false) {
                    moved.add(result);
                } else {
                    refactoredInPlace.add(result);
                }
            }
        }

        public Path getProjectRoot() {
            return projectRoot;
        }

        public boolean isNotEmpty() {
            return generated.isEmpty() == false
                    || deleted.isEmpty() == false
                    || moved.isEmpty() == false
                    || refactoredInPlace.isEmpty() == false;
        }
    }
}
