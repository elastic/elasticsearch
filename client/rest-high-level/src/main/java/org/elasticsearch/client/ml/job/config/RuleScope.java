/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RuleScope implements ToXContentObject {

    public static ContextParser<Void, RuleScope> parser() {
        return (p, c) -> {
            Map<String, Object> unparsedScope = p.map();
            if (unparsedScope.isEmpty()) {
                return new RuleScope();
            }
            Map<String, FilterRef> scope = new HashMap<>();
            for (Map.Entry<String, Object> entry : unparsedScope.entrySet()) {
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    @SuppressWarnings("unchecked")
                    Map<String, ?> value = (Map<String, ?>) entry.getValue();
                    builder.map(value);
                    try (XContentParser scopeParser = XContentFactory.xContent(builder.contentType()).createParser(
                            NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, Strings.toString(builder))) {
                        scope.put(entry.getKey(), FilterRef.PARSER.parse(scopeParser, null));
                    }
                }
            }
            return new RuleScope(scope);
        };
    }

    private final Map<String, FilterRef> scope;

    public RuleScope() {
        scope = Collections.emptyMap();
    }

    public RuleScope(Map<String, FilterRef> scope) {
        this.scope = Collections.unmodifiableMap(scope);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(scope);
    }

    public boolean isEmpty() {
        return scope.isEmpty();
    }

    public Set<String> getReferencedFilters() {
        return scope.values().stream().map(FilterRef::getFilterId).collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof RuleScope == false) {
            return false;
        }

        RuleScope other = (RuleScope) obj;
        return Objects.equals(scope, other.scope);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Map<String, FilterRef> scope = new HashMap<>();

        public Builder() {
        }

        public Builder(RuleScope otherScope) {
            scope = new HashMap<>(otherScope.scope);
        }

        public Builder exclude(String field, String filterId) {
            scope.put(field, new FilterRef(filterId, FilterRef.FilterType.EXCLUDE));
            return this;
        }

        public Builder include(String field, String filterId) {
            scope.put(field, new FilterRef(filterId, FilterRef.FilterType.INCLUDE));
            return this;
        }

        public RuleScope build() {
            return new RuleScope(scope);
        }
    }
}
