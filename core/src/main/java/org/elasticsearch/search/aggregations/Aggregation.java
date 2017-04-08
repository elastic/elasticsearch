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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.ParseField;

import java.util.Map;

/**
 * An aggregation
 */
public interface Aggregation {

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
     * Get the optional byte array metadata that was set on the aggregation
     */
    Map<String, Object> getMetaData();

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
    }
}
