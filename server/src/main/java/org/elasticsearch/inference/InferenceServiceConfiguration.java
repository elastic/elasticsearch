/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the configuration field settings for a connector.
 */
public class InferenceServiceConfiguration implements Writeable, ToXContentObject {

    private final String provider;
    private final EnumSet<TaskType> taskTypes;
    private final Map<String, ServiceConfiguration> configuration;

    /**
     * Constructs a new {@link InferenceServiceConfiguration} instance with specified properties.
     *
     * @param provider       The name of the service provider.
     * @param taskTypes      A list of {@link TaskType} supported by the service provider.
     * @param configuration  The configuration of the service provider, defined by {@link ServiceConfiguration}.
     */
    private InferenceServiceConfiguration(String provider, EnumSet<TaskType> taskTypes, Map<String, ServiceConfiguration> configuration) {
        this.provider = provider;
        this.taskTypes = taskTypes;
        this.configuration = configuration;
    }

    static final ParseField PROVIDER_FIELD = new ParseField("provider");
    static final ParseField TASK_TYPES_FIELD = new ParseField("task_types");
    static final ParseField CONFIGURATION_FIELD = new ParseField("configuration");

    public String getProvider() {
        return provider;
    }

    public EnumSet<TaskType> getTaskTypes() {
        return taskTypes;
    }

    public Map<String, ServiceConfiguration> getConfiguration() {
        return configuration;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(PROVIDER_FIELD.getPreferredName(), provider);
            builder.field(TASK_TYPES_FIELD.getPreferredName(), taskTypes);
            builder.field(CONFIGURATION_FIELD.getPreferredName(), configuration);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(provider);
        out.writeCollection(taskTypes);
        out.writeGenericValue(configuration);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(PROVIDER_FIELD.getPreferredName(), provider);
        map.put(TASK_TYPES_FIELD.getPreferredName(), taskTypes);
        map.put(CONFIGURATION_FIELD.getPreferredName(), configuration);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceServiceConfiguration that = (InferenceServiceConfiguration) o;
        return provider.equals(that.provider)
            && Objects.equals(taskTypes, that.taskTypes)
            && Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, taskTypes, configuration);
    }

    public static class Builder {

        private String provider;
        private EnumSet<TaskType> taskTypes;
        private Map<String, ServiceConfiguration> configuration;

        public Builder setProvider(String provider) {
            this.provider = provider;
            return this;
        }

        public Builder setTaskTypes(EnumSet<TaskType> taskTypes) {
            this.taskTypes = taskTypes;
            return this;
        }

        public Builder setConfiguration(Map<String, ServiceConfiguration> configuration) {
            this.configuration = configuration;
            return this;
        }

        public InferenceServiceConfiguration build() {
            return new InferenceServiceConfiguration(provider, taskTypes, configuration);
        }
    }
}
