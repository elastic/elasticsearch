/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents the configuration field settings for a specific task type inference provider.
 */
public class TaskSettingsConfiguration implements Writeable, ToXContentObject {

    private final TaskType taskType;
    private final Map<String, SettingsConfiguration> configuration;

    /**
     * Constructs a new {@link TaskSettingsConfiguration} instance with specified properties.
     *
     * @param taskType       The {@link TaskType} this configuration describes.
     * @param configuration  The configuration of the task, defined by {@link SettingsConfiguration}.
     */
    private TaskSettingsConfiguration(TaskType taskType, Map<String, SettingsConfiguration> configuration) {
        this.taskType = taskType;
        this.configuration = configuration;
    }

    public TaskSettingsConfiguration(StreamInput in) throws IOException {
        this.taskType = in.readEnum(TaskType.class);
        this.configuration = in.readMap(SettingsConfiguration::new);
    }

    static final ParseField TASK_TYPE_FIELD = new ParseField("task_type");
    static final ParseField CONFIGURATION_FIELD = new ParseField("configuration");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TaskSettingsConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        "task_configuration",
        true,
        args -> {
            return new TaskSettingsConfiguration.Builder().setTaskType(TaskType.fromString((String) args[0]))
                .setConfiguration((Map<String, SettingsConfiguration>) args[1])
                .build();
        }
    );

    static {
        PARSER.declareString(constructorArg(), TASK_TYPE_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), CONFIGURATION_FIELD);
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public Map<String, SettingsConfiguration> getConfiguration() {
        return configuration;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(TASK_TYPE_FIELD.getPreferredName(), taskType);
            builder.field(CONFIGURATION_FIELD.getPreferredName(), configuration);
        }
        builder.endObject();
        return builder;
    }

    public static TaskSettingsConfiguration fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static TaskSettingsConfiguration fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return TaskSettingsConfiguration.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse task configuration", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(taskType);
        out.writeMapValues(configuration);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(TASK_TYPE_FIELD.getPreferredName(), taskType);
        map.put(CONFIGURATION_FIELD.getPreferredName(), configuration);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskSettingsConfiguration that = (TaskSettingsConfiguration) o;
        return Objects.equals(taskType, that.taskType) && Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskType, configuration);
    }

    public static class Builder {

        private TaskType taskType;
        private Map<String, SettingsConfiguration> configuration;

        public Builder setTaskType(TaskType taskType) {
            this.taskType = taskType;
            return this;
        }

        public Builder setConfiguration(Map<String, SettingsConfiguration> configuration) {
            this.configuration = configuration;
            return this;
        }

        public TaskSettingsConfiguration build() {
            return new TaskSettingsConfiguration(taskType, configuration);
        }
    }
}
