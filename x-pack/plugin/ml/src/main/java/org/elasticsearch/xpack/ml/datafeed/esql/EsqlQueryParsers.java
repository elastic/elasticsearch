/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql;

import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;

import java.util.List;
import java.util.Optional;

public final class EsqlQueryParsers {

    private EsqlQueryParsers() {}

    /** The one provider, or empty. Instances of one class count as one. Throws when the providers are of different classes. */
    public static Optional<EsqlQueryParser> selectOne(List<? extends EsqlQueryParser> providers) {
        if (providers.stream().map(Object::getClass).distinct().count() > 1) {
            throw new IllegalStateException("Expected one ES|QL query parser provider but found " + providers);
        }
        return providers.stream().findFirst().map(provider -> provider);
    }
}
