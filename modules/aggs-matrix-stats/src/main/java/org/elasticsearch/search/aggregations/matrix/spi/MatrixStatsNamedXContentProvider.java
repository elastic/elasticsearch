/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.matrix.spi;

import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.matrix.stats.ParsedMatrixStats;

import java.util.List;

import static java.util.Collections.singletonList;

public class MatrixStatsNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        ParseField parseField = new ParseField(MatrixStatsAggregationBuilder.NAME);
        ContextParser<Object, Aggregation> contextParser = (p, name) -> ParsedMatrixStats.fromXContent(p, (String) name);
        return singletonList(new NamedXContentRegistry.Entry(Aggregation.class, parseField, contextParser));
    }
}
