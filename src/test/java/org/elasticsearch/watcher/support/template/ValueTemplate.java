/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ValueTemplate implements Template {

    private final String value;

    public ValueTemplate(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public String render(Map<String, Object> model) {
        return value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueTemplate that = (ValueTemplate) o;

        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    public static class Parser implements Template.Parser {

        @Override
        public Template parse(XContentParser parser) throws IOException, ParseException {
            String value = parser.text();
            return new ValueTemplate(value);
        }
    }

    public static class SourceBuilder implements Template.SourceBuilder {

        private final String value;

        public SourceBuilder(String value) {
            this.value = value;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(value);
        }
    }
}
