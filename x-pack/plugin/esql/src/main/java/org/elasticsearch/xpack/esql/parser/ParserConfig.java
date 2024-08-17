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

    private EsqlConfig config;

    public ParserConfig(TokenStream input) {
        super(input);
    }

    boolean devVersion() {
        return config.devVersion;
    }

    boolean releaseVersion() {
        return devVersion() == false;
    }

    boolean hasFeature(String featureName) {
        return config.hasFeature(featureName);
    }

    void setEsqlConfig(EsqlConfig config) {
        this.config = config;
    }
}
