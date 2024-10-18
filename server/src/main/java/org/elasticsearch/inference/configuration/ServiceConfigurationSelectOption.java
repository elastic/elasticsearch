/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ServiceConfigurationSelectOption implements Writeable, ToXContentObject {
    private final String label;
    private final Object value;

    private ServiceConfigurationSelectOption(String label, Object value) {
        this.label = label;
        this.value = value;
    }

    private static final ParseField LABEL_FIELD = new ParseField("label");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(LABEL_FIELD.getPreferredName(), label);
            builder.field(VALUE_FIELD.getPreferredName(), value);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(label);
        out.writeGenericValue(value);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(LABEL_FIELD.getPreferredName(), label);
        map.put(VALUE_FIELD.getPreferredName(), value);
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceConfigurationSelectOption that = (ServiceConfigurationSelectOption) o;
        return Objects.equals(label, that.label) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, value);
    }

    public static class Builder {

        private String label;
        private Object value;

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setValue(Object value) {
            this.value = value;
            return this;
        }

        public Builder setLabelAndValue(String labelAndValue) {
            this.label = labelAndValue;
            this.value = labelAndValue;
            return this;
        }

        public ServiceConfigurationSelectOption build() {
            return new ServiceConfigurationSelectOption(label, value);
        }
    }

}
