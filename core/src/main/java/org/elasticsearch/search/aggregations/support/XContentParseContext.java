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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * A minimal context for parsing xcontent into aggregation builders.
 * Only a minimal set of dependencies and settings are available.
 */
public final class XContentParseContext {

    private final XContentParser parser;

    private final ParseFieldMatcher parseFieldMatcher;

    private final String defaultScriptLanguage;

    public XContentParseContext(XContentParser parser, ParseFieldMatcher parseFieldMatcher, String defaultScriptLanguage) {
        this.parser = parser;
        this.parseFieldMatcher = parseFieldMatcher;
        this.defaultScriptLanguage = defaultScriptLanguage;
    }

    public XContentParser getParser() {
        return parser;
    }

    public ParseFieldMatcher getParseFieldMatcher() {
        return parseFieldMatcher;
    }

    public String getDefaultScriptLanguage() {
        return defaultScriptLanguage;
    }

    /**
     * Returns whether the parse field we're looking for matches with the found field name.
     *
     * Helper that delegates to {@link ParseFieldMatcher#match(String, ParseField)}.
     */
    public boolean matchField(String fieldName, ParseField parseField) {
        return parseFieldMatcher.match(fieldName, parseField);
    }

}
