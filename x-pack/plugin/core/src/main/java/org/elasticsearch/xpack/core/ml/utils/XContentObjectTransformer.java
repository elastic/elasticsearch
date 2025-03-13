/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.deprecation.LoggingDeprecationAccumulationHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    public static XContentObjectTransformer<AggregatorFactories.Builder> aggregatorTransformer(NamedXContentRegistry registry) {
        return new XContentObjectTransformer<>(registry, (p) -> {
            // Serializing a map creates an object, need to skip the start object for the aggregation parser
            XContentParser.Token token = p.nextToken();
            assert (XContentParser.Token.START_OBJECT.equals(token));
            return AggregatorFactories.parseAggregators(p);
        });
    }

    public static XContentObjectTransformer<QueryBuilder> queryBuilderTransformer(NamedXContentRegistry registry) {
        return new XContentObjectTransformer<>(registry, AbstractQueryBuilder::parseTopLevelQuery);
    }

    XContentObjectTransformer(NamedXContentRegistry registry, CheckedFunction<XContentParser, T, IOException> parserFunction) {
        this.parserFunction = parserFunction;
        this.registry = registry;
    }

    /**
     * Parses the map into the type T with the previously supplied parserFunction
     * All deprecation warnings are ignored
     * @param stringObjectMap The Map to parse into the Object
     * @return parsed object T
     * @throws IOException When there is an unforeseen parsing issue
     */
    public T fromMap(Map<String, Object> stringObjectMap) throws IOException {
        return fromMap(stringObjectMap, new ArrayList<>());
    }

    /**
     * Parses the map into the type T with the previously supplied parserFunction
     * All deprecation warnings are added to the passed deprecationWarnings list.
     *
     * @param stringObjectMap The Map to parse into the Object
     * @param deprecationWarnings The list to which to add all deprecation warnings
     * @return parsed object T
     * @throws IOException When there is an unforeseen parsing issue
     */
    public T fromMap(Map<String, Object> stringObjectMap, List<String> deprecationWarnings) throws IOException {
        if (stringObjectMap == null) {
            return null;
        }
        LoggingDeprecationAccumulationHandler deprecationLogger = new LoggingDeprecationAccumulationHandler();
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(stringObjectMap);
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY.withRegistry(registry).withDeprecationHandler(deprecationLogger),
                BytesReference.bytes(xContentBuilder),
                XContentType.JSON
            )
        ) {
            T retVal = parserFunction.apply(parser);
            deprecationWarnings.addAll(deprecationLogger.getDeprecations());
            return retVal;
        }
    }

    public Map<String, Object> toMap(T object) throws IOException {
        if (object == null) {
            return null;
        }
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = object.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2();
        }
    }

}
