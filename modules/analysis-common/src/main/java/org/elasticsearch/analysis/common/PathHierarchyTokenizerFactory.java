/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

public class PathHierarchyTokenizerFactory extends AbstractTokenizerFactory {

    private final int bufferSize;

    private final char delimiter;
    private final char replacement;
    private final int skip;
    private final boolean reverse;

    PathHierarchyTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        bufferSize = settings.getAsInt("buffer_size", 1024);
        String delimiterString = settings.get("delimiter");
        if (delimiterString == null) {
            this.delimiter = PathHierarchyTokenizer.DEFAULT_DELIMITER;
        } else if (delimiterString.length() != 1) {
            throw new IllegalArgumentException("delimiter must be a one char value");
        } else {
            this.delimiter = delimiterString.charAt(0);
        }

        String replacementString = settings.get("replacement");
        if (replacementString == null) {
            this.replacement = this.delimiter;
        } else if (replacementString.length() != 1) {
            throw new IllegalArgumentException("replacement must be a one char value");
        } else {
            this.replacement = replacementString.charAt(0);
        }
        this.skip = settings.getAsInt("skip", PathHierarchyTokenizer.DEFAULT_SKIP);
        this.reverse = settings.getAsBoolean("reverse", false);
    }

    @Override
    public Tokenizer create() {
        if (reverse) {
            return new ReversePathHierarchyTokenizer(bufferSize, delimiter, replacement, skip);
        }
        return new PathHierarchyTokenizer(bufferSize, delimiter, replacement, skip);
    }

}
