/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.spi;

import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.join.aggregations.ParentAggregationBuilder;
import org.elasticsearch.join.aggregations.ParsedChildren;
import org.elasticsearch.join.aggregations.ParsedParent;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.Arrays;
import java.util.List;

public class ParentJoinNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        ParseField parseFieldChildren = new ParseField(ChildrenAggregationBuilder.NAME);
        ParseField parseFieldParent = new ParseField(ParentAggregationBuilder.NAME);
        ContextParser<Object, Aggregation> contextParserChildren = (p, name) -> ParsedChildren.fromXContent(p, (String) name);
        ContextParser<Object, Aggregation> contextParserParent = (p, name) -> ParsedParent.fromXContent(p, (String) name);
        return Arrays.asList(
            new NamedXContentRegistry.Entry(Aggregation.class, parseFieldChildren, contextParserChildren),
            new NamedXContentRegistry.Entry(Aggregation.class, parseFieldParent, contextParserParent)
        );
    }
}
