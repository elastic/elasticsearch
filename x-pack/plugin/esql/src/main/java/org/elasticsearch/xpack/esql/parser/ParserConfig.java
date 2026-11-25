/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.TokenStream;

public abstract class ParserConfig extends Parser {

    // is null when running inside the IDEA plugin
    private EsqlConfig config;

    public ParserConfig(TokenStream input) {
        super(input);
    }

    boolean isDevVersion() {
        return config == null || config.isDevVersion();
    }

    void setEsqlConfig(EsqlConfig config) {
        this.config = config;
    }
}
