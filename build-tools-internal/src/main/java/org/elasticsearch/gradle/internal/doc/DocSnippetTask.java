/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc;

import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.List;

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
    abstract MapProperty<String, String> getDefaultSubstitutions();

    @TaskAction
    void executeTask() {
        for (File file : docs) {
            List<Snippet> snippets = parseDocFile(docs.getDir(), file);
            if (perSnippet != null) {
                snippets.forEach(perSnippet::execute);
            }
        }
    }

    List<Snippet> parseDocFile(File rootDir, File docFile) {
        SnippetParser parser = parserForFileType(docFile);
        return parser.parseDoc(rootDir, docFile);
    }

    private SnippetParser parserForFileType(File docFile) {
        if (docFile.getName().endsWith(".asciidoc")) {
            return new AsciidocSnippetParser(getDefaultSubstitutions().get());
        } else if (docFile.getName().endsWith(".mdx")) {
            return new MdxSnippetParser(getDefaultSubstitutions().get());
        }
        throw new InvalidUserDataException("Unsupported file type: " + docFile.getName());
    }

    public void setPerSnippet(Action<Snippet> perSnippet) {
        this.perSnippet = perSnippet;
    }

}
