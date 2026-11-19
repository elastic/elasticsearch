/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;

import java.util.Objects;

/**
 * Makes the {@link EsqlQueryParser} injectable into transport actions: Guice binds plugin components by their concrete class.
 */
public record EsqlQueryParserHolder(EsqlQueryParser parser) {

    public EsqlQueryParserHolder {
        Objects.requireNonNull(parser);
    }
}
