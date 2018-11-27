/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * This is a utility class that allows simple one-to-one transformations between an ToXContentObject type
 * to and from other supported objects.
 *
 * @param <T> The type of the object that we will be transforming to/from
 */
public class XContentObjectTransformer<T extends ToXContentObject> {

    private final NamedXContentRegistry registry;
    private final CheckedFunction<XContentParser, T, IOException> parserFunction;

    // We need this registry for parsing out Aggregations and Searches
    private static NamedXContentRegistry searchRegistry;
    static {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        searchRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public static XContentObjectTransformer<AggregatorFactories.Builder> aggregatorTransformer() {
        return new XContentObjectTransformer<>(searchRegistry, (p) -> {
            // Serializing a map creates an object, need to skip the start object for the aggregation parser
            assert(XContentParser.Token.START_OBJECT.equals(p.nextToken()));
            return AggregatorFactories.parseAggregators(p);
        });
    }

    public static XContentObjectTransformer<QueryBuilder> queryBuilderTransformer() {
        return new XContentObjectTransformer<>(searchRegistry, AbstractQueryBuilder::parseInnerQueryBuilder);
    }

    XContentObjectTransformer(NamedXContentRegistry registry, CheckedFunction<XContentParser, T, IOException> parserFunction) {
        this.parserFunction = parserFunction;
        this.registry = registry;
    }

    public T fromMap(Map<String, Object> stringObjectMap) throws IOException {
        try(XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(stringObjectMap);
            XContentParser parser = XContentType.JSON
                .xContent()
                .createParser(registry,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(xContentBuilder).streamInput())) {
            return parserFunction.apply(parser);
        }
    }

    public Map<String, Object> toMap(T object) throws IOException {
        try(XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = object.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2();
        }
    }

}
