/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;

import java.util.Map;

/**
 * An aggregation. Extends {@link ToXContent} as it makes it easier to print out its content.
 */
public interface Aggregation extends ToXContentFragment {

    /**
     * Delimiter used when prefixing aggregation names with their type
     * using the typed_keys parameter
     */
    String TYPED_KEYS_DELIMITER = "#";

    /**
     * @return The name of this aggregation.
     */
    String getName();

    /**
     * @return a string representing the type of the aggregation. This type is added to
     * the aggregation name in the response, so that it can later be used by clients
     * to determine type of the aggregation and parse it into the proper object.
     */
    String getType();

    /**
     * Get the optional byte array metadata that was set on the aggregation
     */
    Map<String, Object> getMetadata();

    /**
     * Common xcontent fields that are shared among addAggregation
     */
    final class CommonFields extends ParseField.CommonFields {
        public static final ParseField META = new ParseField("meta");
        public static final ParseField BUCKETS = new ParseField("buckets");
        public static final ParseField VALUE = new ParseField("value");
        public static final ParseField VALUES = new ParseField("values");
        public static final ParseField VALUE_AS_STRING = new ParseField("value_as_string");
        public static final ParseField DOC_COUNT = new ParseField("doc_count");
        public static final ParseField KEY = new ParseField("key");
        public static final ParseField KEY_AS_STRING = new ParseField("key_as_string");
        public static final ParseField FROM = new ParseField("from");
        public static final ParseField FROM_AS_STRING = new ParseField("from_as_string");
        public static final ParseField TO = new ParseField("to");
        public static final ParseField TO_AS_STRING = new ParseField("to_as_string");
        public static final ParseField MIN = new ParseField("min");
        public static final ParseField MIN_AS_STRING = new ParseField("min_as_string");
        public static final ParseField MAX = new ParseField("max");
        public static final ParseField MAX_AS_STRING = new ParseField("max_as_string");
    }
}
