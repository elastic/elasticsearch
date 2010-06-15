/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.builder.TextXContentBuilder;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.util.TimeValue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.Actions.*;
import static org.elasticsearch.common.collect.Maps.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * A request to create an index. Best created with {@link org.elasticsearch.client.Requests#createIndexRequest(String)}.
 *
 * <p>The index created can optionally be created with {@link #settings(org.elasticsearch.common.settings.Settings)}.
 *
 * @author kimchy (shay.banon)
 * @see org.elasticsearch.client.IndicesAdminClient#create(CreateIndexRequest)
 * @see org.elasticsearch.client.Requests#createIndexRequest(String)
 * @see CreateIndexResponse
 */
public class CreateIndexRequest extends MasterNodeOperationRequest {

    private String cause = "";

    private String index;

    private Settings settings = EMPTY_SETTINGS;

    private Map<String, String> mappings = newHashMap();

    private TimeValue timeout = new TimeValue(10, TimeUnit.SECONDS);

    CreateIndexRequest() {
    }

    /**
     * Constructs a new request to create an index with the specified name.
     */
    public CreateIndexRequest(String index) {
        this(index, EMPTY_SETTINGS);
    }

    /**
     * Constructs a new request to create an index with the specified name and settings.
     */
    public CreateIndexRequest(String index, Settings settings) {
        this.index = index;
        this.settings = settings;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    /**
     * The index name to create.
     */
    String index() {
        return index;
    }

    /**
     * The settings to created the index with.
     */
    Settings settings() {
        return settings;
    }

    /**
     * The cause for this index creation.
     */
    String cause() {
        return cause;
    }

    /**
     * The settings to created the index with.
     */
    public CreateIndexRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to created the index with.
     */
    public CreateIndexRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to crete the index with (either json/yaml/properties format)
     */
    public CreateIndexRequest settings(String source) {
        this.settings = ImmutableSettings.settingsBuilder().loadFromSource(source).build();
        return this;
    }

    /**
     * The settings to crete the index with (either json/yaml/properties format)
     */
    public CreateIndexRequest settings(Map source) {
        try {
            TextXContentBuilder builder = XContentFactory.contentTextBuilder(XContentType.JSON);
            builder.map(source);
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(String type, String source) {
        mappings.put(type, source);
        return this;
    }

    /**
     * The cause for this index creation.
     */
    public CreateIndexRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(String type, XContentBuilder source) {
        try {
            mappings.put(type, source.string());
        } catch (IOException e) {
            throw new ElasticSearchIllegalArgumentException("Failed to build json for mapping request", e);
        }
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(String type, Map source) {
        try {
            TextXContentBuilder builder = XContentFactory.contentTextBuilder(XContentType.JSON);
            builder.map(source);
            return mapping(type, builder.string());
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    Map<String, String> mappings() {
        return this.mappings;
    }

    /**
     * Timeout to wait for the index creation to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    TimeValue timeout() {
        return timeout;
    }

    /**
     * Timeout to wait for the index creation to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public CreateIndexRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Timeout to wait for the index creation to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public CreateIndexRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null));
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        cause = in.readUTF();
        index = in.readUTF();
        settings = readSettingsFromStream(in);
        timeout = readTimeValue(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            mappings.put(in.readUTF(), in.readUTF());
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(cause);
        out.writeUTF(index);
        writeSettingsToStream(settings, out);
        timeout.writeTo(out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }
}