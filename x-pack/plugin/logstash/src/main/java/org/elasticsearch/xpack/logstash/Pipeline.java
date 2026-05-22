/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;

import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class Pipeline {

    public static final int MAX_DESCRIPTION_LENGTH = 1_024;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Pipeline, String> PARSER = new ConstructingObjectParser<>(
        "pipeline",
        true,
        (objects, id) -> {
            Iterator<Object> iterator = Arrays.asList(objects).iterator();
            return new Pipeline(
                id,
                (Instant) iterator.next(),
                (Map<String, Object>) iterator.next(),
                (String) iterator.next(),
                (String) iterator.next(),
                (Map<String, Object>) iterator.next(),
                (String) iterator.next()
            );
        }
    );

    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField LAST_MODIFIED = new ParseField("last_modified");
    public static final ParseField PIPELINE_METADATA = new ParseField("pipeline_metadata");
    public static final ParseField USERNAME = new ParseField("username");
    public static final ParseField PIPELINE = new ParseField("pipeline");
    public static final ParseField PIPELINE_SETTINGS = new ParseField("pipeline_settings");

    static {
        PARSER.declareField(constructorArg(), (parser, s) -> {
            final String instantISOString = parser.text();
            return Instant.parse(instantISOString);
        }, LAST_MODIFIED, ValueType.STRING);
        PARSER.declareObject(constructorArg(), (parser, s) -> parser.map(), PIPELINE_METADATA);
        PARSER.declareString(constructorArg(), USERNAME);
        PARSER.declareString(constructorArg(), PIPELINE);
        PARSER.declareObject(constructorArg(), (parser, s) -> parser.map(), PIPELINE_SETTINGS);
        PARSER.declareField(optionalConstructorArg(), (parser, s) -> validateDescription(parser.text()), DESCRIPTION, ValueType.STRING);
    }

    private final String id;
    private final Instant lastModified;
    private final Map<String, Object> pipelineMetadata;
    private final String username;
    private final String pipeline;
    private final Map<String, Object> pipelineSettings;
    private final String description;

    public Pipeline(
        String id,
        Instant lastModified,
        Map<String, Object> pipelineMetadata,
        String username,
        String pipeline,
        Map<String, Object> pipelineSettings,
        String description
    ) {
        this.id = id;
        this.lastModified = lastModified;
        this.pipelineMetadata = pipelineMetadata;
        this.username = username;
        this.pipeline = pipeline;
        this.pipelineSettings = pipelineSettings;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public Instant getLastModified() {
        return lastModified;
    }

    public Map<String, Object> getPipelineMetadata() {
        return pipelineMetadata;
    }

    public String getUsername() {
        return username;
    }

    public String getPipeline() {
        return pipeline;
    }

    public Map<String, Object> getPipelineSettings() {
        return pipelineSettings;
    }

    public String getDescription() {
        return description;
    }

    private static String validateDescription(String description) {
        if (description.length() > MAX_DESCRIPTION_LENGTH) {
            throw new IllegalArgumentException("[description] must be less than 1024 characters in length.");
        }
        return description;
    }
}
