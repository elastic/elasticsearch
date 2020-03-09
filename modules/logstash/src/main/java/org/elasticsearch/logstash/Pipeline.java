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

package org.elasticsearch.logstash;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;

import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class Pipeline {

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
                (Map<String, Object>) iterator.next()
            );
        }
    );

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
    }

    private final String id;
    private final Instant lastModified;
    private final Map<String, Object> pipelineMetadata;
    private final String username;
    private final String pipeline;
    private final Map<String, Object> pipelineSettings;

    public Pipeline(
        String id,
        Instant lastModified,
        Map<String, Object> pipelineMetadata,
        String username,
        String pipeline,
        Map<String, Object> pipelineSettings
    ) {
        this.id = id;
        this.lastModified = lastModified;
        this.pipelineMetadata = pipelineMetadata;
        this.username = username;
        this.pipeline = pipeline;
        this.pipelineSettings = pipelineSettings;
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
}
