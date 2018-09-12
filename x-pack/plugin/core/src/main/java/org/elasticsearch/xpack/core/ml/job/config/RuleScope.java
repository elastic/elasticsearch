/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RuleScope implements ToXContentObject, Writeable {

    public static ContextParser<Void, RuleScope> parser(boolean ignoreUnknownFields) {
        return (p, c) -> {
            Map<String, Object> unparsedScope = p.map();
            if (unparsedScope.isEmpty()) {
                return new RuleScope();
            }
            ConstructingObjectParser<FilterRef, Void> filterRefParser =
                ignoreUnknownFields ? FilterRef.LENIENT_PARSER : FilterRef.STRICT_PARSER;
            Map<String, FilterRef> scope = new HashMap<>();
            for (Map.Entry<String, Object> entry : unparsedScope.entrySet()) {
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    builder.map((Map<String, ?>) entry.getValue());
                    try (XContentParser scopeParser = XContentFactory.xContent(builder.contentType()).createParser(
                            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, Strings.toString(builder))) {
                        scope.put(entry.getKey(), filterRefParser.parse(scopeParser, null));
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

    public RuleScope(StreamInput in) throws IOException {
        scope = in.readMap(StreamInput::readString, FilterRef::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(scope, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(scope);
    }

    public boolean isEmpty() {
        return scope.isEmpty();
    }

    public void validate(Set<String> validKeys) {
        Optional<String> invalidKey = scope.keySet().stream().filter(k -> !validKeys.contains(k)).findFirst();
        if (invalidKey.isPresent()) {
            if (validKeys.isEmpty()) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_SCOPE_NO_AVAILABLE_FIELDS,
                        invalidKey.get()));
            }
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_SCOPE_HAS_INVALID_FIELD,
                    invalidKey.get(), validKeys));
        }
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

        public Builder() {}

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
