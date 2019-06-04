/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.spi;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.xpack.ml.aggregations.pca.PCAAggregationBuilder;
import org.elasticsearch.xpack.ml.aggregations.pca.ParsedPCAStats;

import java.util.List;

import static java.util.Collections.singletonList;

public class MLNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        ParseField parseField = new ParseField(PCAAggregationBuilder.NAME);
        ContextParser<Object, Aggregation> contextParser = (p, name) -> ParsedPCAStats.fromXContent(p, (String) name);
        return singletonList(new NamedXContentRegistry.Entry(Aggregation.class, parseField, contextParser));
    }
}
