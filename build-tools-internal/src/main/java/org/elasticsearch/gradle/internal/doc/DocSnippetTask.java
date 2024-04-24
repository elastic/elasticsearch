/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

import org.apache.commons.collections.map.HashedMap;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class DocSnippetTask extends DefaultTask {

    /**
     * Action to take on each snippet. Called with a single parameter, an
     * instance of Snippet.
     */
    private Action<Snippet> perSnippet;

    /**
     * The docs to scan. Defaults to every file in the directory exception the
     * build.gradle file because that is appropriate for Elasticsearch's docs
     * directory.
     */
    private ConfigurableFileTree docs;
    private Map<String, String> defaultSubstitutions = new HashedMap();

    @InputFiles
    public ConfigurableFileTree getDocs() {
        return docs;
    }

    public void setDocs(ConfigurableFileTree docs) {
        this.docs = docs;
    }

    /**
     * Substitutions done on every snippet's contents.
     */
    @Input
    public Map<String, String> getDefaultSubstitutions() {
        return defaultSubstitutions;
    }

    @TaskAction
    void executeTask() {
        for (File file : docs) {
            List<Snippet> snippets = parseDocFile(docs.getDir(), file, new ArrayList<>());
            if (perSnippet != null) {
                snippets.forEach(perSnippet::execute);
            }
        }
    }

    List<Snippet> parseDocFile(File rootDir, File docFile, List<Map.Entry<String, String>> substitutions) {
        SnippetParser parser = parserForFileType(docFile);
        return parser.parseDoc(rootDir, docFile, substitutions);
    }

    private SnippetParser parserForFileType(File docFile) {
        if (docFile.getName().endsWith(".asciidoc")) {
            return new AsciidocSnippetParser(defaultSubstitutions);
        }
        throw new InvalidUserDataException("Unsupported file type: " + docFile.getName());
    }

    public void setDefaultSubstitutions(Map<String, String> defaultSubstitutions) {
        this.defaultSubstitutions = defaultSubstitutions;
    }

    public void setPerSnippet(Action<Snippet> perSnippet) {
        this.perSnippet = perSnippet;
    }

}
