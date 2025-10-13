/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;

/**
 * Base class for hooking versioning information into the ANTLR parser.
 */
public abstract class LexerConfig extends Lexer {

    // is null when running inside the IDEA plugin
    EsqlConfig config;

    public LexerConfig() {}

    public LexerConfig(CharStream input) {
        super(input);
    }

    boolean isDevVersion() {
        return config == null || config.isDevVersion();
    }

    void setEsqlConfig(EsqlConfig config) {
        this.config = config;
    }
}
