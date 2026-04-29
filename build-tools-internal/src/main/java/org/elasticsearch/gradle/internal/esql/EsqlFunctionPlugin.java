/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.esql;

import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.util.SourceDirectoryCommandLineArgumentProvider;
import org.elasticsearch.gradle.util.PlatformUtils;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.Transformer;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.FileTree;
import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.plugins.ide.idea.IdeaPlugin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Configures a project to create ESQL scalar and aggregate functions.
 * Also configures standard function testing.
 */
public class EsqlFunctionPlugin implements Plugin<Project> {

    private static final List<String> DOC_FOLDERS = List.of("esql", "promql");
    private static final String REPLACEMENT_FONT_FAMILY = """
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;\
        """;

    interface Injected {
        @Inject
        FileSystemOperations getFs();
    }

    @Override
    public void apply(Project project) {
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        boolean isCi = loadBuildParams(project).get().getCi();

        if (project.getPath().equals(":x-pack:plugin:esql") == false) {
            project.getDependencies().add("implementation", project.project(":x-pack:plugin:esql"));
            project.getDependencies().add("testImplementation", project.project(":x-pack:plugin:esql:qa:testFixtures"));
        }
        project.getDependencies().add("implementation", project.project(":x-pack:plugin:esql:compute"));
        project.getDependencies().add("implementation", project.project(":x-pack:plugin:esql:compute:ann"));
        project.getDependencies().add("annotationProcessor", project.project(":x-pack:plugin:esql:compute:gen"));

        String generatedPath = "src/main/generated";
        Directory generatedSourceDir = project.getLayout().getProjectDirectory().dir(generatedPath);
        project.getTasks().named("compileJava", JavaCompile.class).configure(compileJava -> {
            compileJava.getOptions().getCompilerArgumentProviders().add(new SourceDirectoryCommandLineArgumentProvider(generatedSourceDir));
            // IntelliJ sticks generated files here, and we can't stop it....
            compileJava.exclude(
                element -> PlatformUtils.normalize(element.getFile().toString()).contains("src/main/generated-src/generated")
            );
        });
        project.getPlugins().withType(IdeaPlugin.class, ideaPlugin -> {
            ideaPlugin.getModel().getModule().getSourceDirs().add(project.file(generatedPath));
        });

        project.getTasks().named("test", Test.class).configure(test -> {
            // https://bugs.openjdk.org/browse/JDK-8367990
            // https://github.com/elastic/elasticsearch/issues/135009
            test.jvmArgs("-XX:-OmitStackTraceInFastThrow");

            configureDocGeneration(project, test, isCi);
        });
    }

    private static void configureDocGeneration(Project project, Test test, boolean isCi) {
        Injected injected = project.getObjects().newInstance(Injected.class);

        for (String folder : DOC_FOLDERS) {
            File tempDir = project.file("build/testrun/test/temp/" + folder);
            File commandsExamplesFile = new File(tempDir, "commands.examples");
            FileTree mdFiles = project.fileTree(
                new File(project.getRootDir(), "docs/reference/query-languages/" + folder + "/_snippets/commands/examples/"),
                tree -> tree.include("**/*.csv-spec/*.md")
            );

            Path docFolder = new File(project.getRootDir(), "docs/reference/query-languages/" + folder).toPath();
            File snippetsDocFolder = docFolder.resolve("_snippets").toFile();
            File imagesDocFolder = docFolder.resolve("images").toFile();
            File kibanaDocFolder = docFolder.resolve("kibana").toFile();
            File snippetsFolder = project.file("build/testrun/test/temp/" + folder + "/_snippets");
            File imagesFolder = project.file("build/testrun/test/temp/" + folder + "/images");
            File kibanaFolder = project.file("build/testrun/test/temp/" + folder + "/kibana");

            test.doFirst(new Action<Task>() {
                @Override
                public void execute(Task t) {
                    injected.getFs().delete(spec -> spec.delete(tempDir));
                    tempDir.mkdirs();
                    writeCommandsExamplesFile(commandsExamplesFile, mdFiles);
                    t.getLogger()
                        .quiet(
                            "File 'commands.examples' created with "
                                + mdFiles.getFiles().size()
                                + " example specifications from csv-spec files."
                        );
                    if (isCi) {
                        injected.getFs().sync(spec -> {
                            spec.from(snippetsDocFolder);
                            spec.into(snippetsFolder);
                        });
                        injected.getFs().sync(spec -> {
                            spec.from(imagesDocFolder);
                            spec.into(imagesFolder);
                        });
                        injected.getFs().sync(spec -> {
                            spec.from(kibanaDocFolder);
                            spec.into(kibanaFolder);
                        });
                    }
                }
            });

            if (isCi) {
                test.systemProperty("generateDocs", "assert");
            } else {
                test.systemProperty("generateDocs", "write");
                FileTree snippetsTree = project.fileTree(snippetsFolder).matching(p -> p.include("**/types/*.md"));
                FileTree settingsTree = project.fileTree(snippetsFolder).matching(p -> p.include("**/settings/*.md"));
                FileTree commandsExamplesTree = project.fileTree(snippetsFolder).matching(p -> p.include("**/*.csv-spec/*.md"));
                FileTree imagesTree = project.fileTree(imagesFolder).matching(p -> p.include("**/*.svg"));
                FileTree kibanaTree = project.fileTree(kibanaFolder).matching(p -> p.include("**/*.json"));

                test.doLast(new Action<Task>() {
                    @Override
                    public void execute(Task t) {
                        Logger logger = t.getLogger();
                        syncSnippets(
                            logger,
                            injected,
                            folder,
                            snippetsTree,
                            settingsTree,
                            commandsExamplesTree,
                            snippetsFolder,
                            snippetsDocFolder
                        );
                        syncImages(logger, injected, folder, imagesTree, imagesFolder, imagesDocFolder);
                        syncKibana(logger, injected, folder, kibanaTree, kibanaFolder, kibanaDocFolder);
                    }
                });
            }
        }
    }

    private static void writeCommandsExamplesFile(File outputFile, FileTree mdFiles) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (File file : mdFiles) {
                writer.write(file.getParentFile().getName() + "/" + file.getName());
                writer.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void syncSnippets(
        Logger logger,
        Injected injected,
        String folder,
        FileTree snippetsTree,
        FileTree settingsTree,
        FileTree commandsExamplesTree,
        File snippetsFolder,
        File snippetsDocFolder
    ) {
        int countSnippets = snippetsTree.getFiles().size();
        int countQuerySettings = settingsTree.getFiles().size();
        int countCommandsExamples = commandsExamplesTree.getFiles().size();
        if (countSnippets == 0 && countCommandsExamples == 0 && countQuerySettings == 0) {
            logger.quiet(folder.toUpperCase(Locale.ROOT) + " Docs: No function/operator snippets created. Skipping sync.");
        } else {
            logger.quiet(
                folder.toUpperCase(Locale.ROOT)
                    + " Docs: Found "
                    + countSnippets
                    + " generated function/operator snippets and "
                    + countCommandsExamples
                    + " command examples to patch into docs"
            );
            injected.getFs().sync(spec -> {
                spec.from(snippetsFolder);
                spec.into(snippetsDocFolder);
                spec.include("**/*.md");
                if (countSnippets <= 100) {
                    spec.preserve(preserveSpec -> preserveSpec.include("**/*.md"));
                } else {
                    spec.preserve(
                        preserveSpec -> preserveSpec.include(
                            "*.md",
                            "**/operators/*.md",
                            "**/operators/**/*.md",
                            "**/lists/*.md",
                            "**/commands/**/*.md",
                            "**/common/**/*.md"
                        )
                    );
                }
            });
        }
    }

    private static void syncImages(
        Logger logger,
        Injected injected,
        String folder,
        FileTree imagesTree,
        File imagesFolder,
        File imagesDocFolder
    ) {
        int countImages = imagesTree.getFiles().size();
        Transformer<String, String> replaceFont = line -> line.replaceAll("font-family:\\s*Roboto Mono[^;]*;", REPLACEMENT_FONT_FAMILY);
        if (countImages == 0) {
            logger.quiet(folder.toUpperCase(Locale.ROOT) + " Docs: No function signatures created. Skipping sync.");
        } else {
            logger.quiet(folder.toUpperCase(Locale.ROOT) + " Docs: Found " + countImages + " generated SVG files to patch into docs");
            injected.getFs().sync(spec -> {
                spec.from(imagesFolder);
                spec.into(imagesDocFolder);
                spec.include("**/*.svg");
                if (countImages <= 100) {
                    spec.preserve(preserveSpec -> preserveSpec.include("**/*.svg"));
                }
                spec.filter(replaceFont);
            });
        }
    }

    private static void syncKibana(
        Logger logger,
        Injected injected,
        String folder,
        FileTree kibanaTree,
        File kibanaFolder,
        File kibanaDocFolder
    ) {
        int countKibana = kibanaTree.getFiles().size();
        Transformer<String, String> replaceLinks = line -> line.replaceAll(
            "\\]\\(/reference/([^)\\s]+)\\.md(#\\S+)?\\)",
            "](https://www.elastic.co/docs/reference/$1$2)"
        );
        if (countKibana == 0) {
            logger.quiet(folder.toUpperCase(Locale.ROOT) + " Docs: No function/operator kibana docs created. Skipping sync.");
        } else {
            logger.quiet(
                folder.toUpperCase(Locale.ROOT) + " Docs: Found " + countKibana + " generated kibana markdown files to patch into docs"
            );
            // Preserve destination subdirectories whose source counterpart produced no files this
            // run — typically because the generating test (e.g. CommandLicenseTests) was muted,
            // skipped, or filtered out. Without this, a full test-suite run would delete their
            // existing JSONs. See https://github.com/elastic/elasticsearch/issues/147402.
            List<String> preservedSubdirs = unpopulatedKibanaSubdirs(kibanaFolder, kibanaDocFolder);
            injected.getFs().sync(spec -> {
                spec.from(kibanaFolder);
                spec.into(kibanaDocFolder);
                spec.include("**/*.md", "**/*.json");
                spec.preserve(preserveSpec -> {
                    if (countKibana <= 100) {
                        preserveSpec.include("**/*.md", "**/*.json");
                    }
                    for (String sub : preservedSubdirs) {
                        preserveSpec.include(sub + "/**");
                    }
                });
                spec.filter(replaceLinks);
            });
        }
    }

    /**
     * Returns subdirectory paths (relative to the kibana root) that exist in the destination but
     * were not populated by the current test run. Callers use these to exclude such subdirs from
     * deletion during sync, so a muted or skipped generator test cannot wipe up-to-date files.
     */
    private static List<String> unpopulatedKibanaSubdirs(File sourceRoot, File destRoot) {
        List<String> result = new ArrayList<>();
        File destDef = new File(destRoot, "definition");
        if (destDef.isDirectory() == false) {
            return result;
        }
        File[] destSubdirs = destDef.listFiles(File::isDirectory);
        if (destSubdirs == null) {
            return result;
        }
        for (File destSub : destSubdirs) {
            String relative = "definition/" + destSub.getName();
            File srcSub = new File(sourceRoot, relative);
            if (containsKibanaContent(srcSub) == false) {
                result.add(relative);
            }
        }
        return result;
    }

    private static boolean containsKibanaContent(File dir) {
        if (dir.isDirectory() == false) {
            return false;
        }
        File[] children = dir.listFiles();
        if (children == null) {
            return false;
        }
        for (File child : children) {
            if (child.isFile()) {
                String n = child.getName();
                if (n.endsWith(".json") || n.endsWith(".md")) {
                    return true;
                }
            } else if (child.isDirectory() && containsKibanaContent(child)) {
                return true;
            }
        }
        return false;
    }

}
