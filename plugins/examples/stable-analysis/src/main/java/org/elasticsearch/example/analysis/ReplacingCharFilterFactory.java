/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.elasticsearch.example.analysis.lucene.ReplaceCharToNumber;
import org.elasticsearch.plugin.NamedComponent;
import org.elasticsearch.plugin.Inject;
import org.elasticsearch.plugin.analysis.CharFilterFactory;

import java.io.Reader;

@NamedComponent( "example_char_filter")
public class ReplacingCharFilterFactory implements CharFilterFactory {
    private final String oldChar;
    private final int newNumber;

    @Inject
    public ReplacingCharFilterFactory(ExampleAnalysisSettings settings) {
        oldChar = settings.oldCharToReplaceInCharFilter();
        newNumber = settings.newNumberToReplaceInCharFilter();
    }

    @Override
    public Reader create(Reader reader) {
        return new ReplaceCharToNumber(reader, oldChar, newNumber);
    }
}


