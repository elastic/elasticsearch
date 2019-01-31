/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.join.spi;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.join.aggregations.ParentAggregationBuilder;
import org.elasticsearch.join.aggregations.ParsedChildren;
import org.elasticsearch.join.aggregations.ParsedParent;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.search.aggregations.Aggregation;

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
