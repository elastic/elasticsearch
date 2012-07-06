/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.google.common.base.Charsets;
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.ImmutableSettings.readSettingsFromStream;
import static org.elasticsearch.common.settings.ImmutableSettings.writeSettingsToStream;
import static org.elasticsearch.common.unit.TimeValue.readTimeValue;

/**
 * A request to create an index. Best created with {@link org.elasticsearch.client.Requests#createIndexRequest(String)}.
 * <p/>
 * <p>The index created can optionally be created with {@link #settings(org.elasticsearch.common.settings.Settings)}.
 *
 * @see org.elasticsearch.client.IndicesAdminClient#create(CreateIndexRequest)
 * @see org.elasticsearch.client.Requests#createIndexRequest(String)
 * @see CreateIndexResponse
 */
public class CreateIndexRequest extends MasterNodeOperationRequest {

    private String cause = "";

    private String index;

    private Settings settings = EMPTY_SETTINGS;

    private Map<String, String> mappings = newHashMap();

    private Map<String, IndexMetaData.Custom> customs = newHashMap();

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

    @Override
    public ActionRequestValidationException validate() {
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

    public CreateIndexRequest index(String index) {
        this.index = index;
        return this;
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
     * Allows to set the settings using a json builder.
     */
    public CreateIndexRequest settings(XContentBuilder builder) {
        try {
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate json settings from builder", e);
        }
        return this;
    }

    /**
     * The settings to crete the index with (either json/yaml/properties format)
     */
    public CreateIndexRequest settings(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
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
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = MapBuilder.<String, Object>newMapBuilder().put(type, source).map();
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            return mapping(type, builder.string());
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(String source) {
        return source(source.getBytes(Charsets.UTF_8));
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(XContentBuilder source) {
        return source(source.bytes());
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    public CreateIndexRequest source(byte[] source, int offset, int length) {
        return source(new BytesArray(source, offset, length));
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(BytesReference source) {
        XContentType xContentType = XContentFactory.xContentType(source);
        if (xContentType != null) {
            try {
                source(XContentFactory.xContent(xContentType).createParser(source).mapAndClose());
            } catch (IOException e) {
                throw new ElasticSearchParseException("failed to parse source for create index", e);
            }
        } else {
            settings(new String(source.toBytes(), Charsets.UTF_8));
        }
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(Map<String, Object> source) {
        boolean found = false;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("settings")) {
                found = true;
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("mappings")) {
                found = true;
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> entry1 : mappings.entrySet()) {
                    mapping(entry1.getKey(), (Map<String, Object>) entry1.getValue());
                }
            } else {
                // maybe custom?
                IndexMetaData.Custom.Factory factory = IndexMetaData.lookupFactory(name);
                if (factory != null) {
                    found = true;
                    try {
                        customs.put(name, factory.fromMap((Map<String, Object>) entry.getValue()));
                    } catch (IOException e) {
                        throw new ElasticSearchParseException("failed to parse custom metadata for [" + name + "]");
                    }
                }
            }
        }
        if (!found) {
            // the top level are settings, use them
            settings(source);
        }
        return this;
    }

    Map<String, String> mappings() {
        return this.mappings;
    }

    public CreateIndexRequest custom(IndexMetaData.Custom custom) {
        customs.put(custom.type(), custom);
        return this;
    }

    Map<String, IndexMetaData.Custom> customs() {
        return this.customs;
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

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    @Override
    public CreateIndexRequest masterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = timeout;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cause = in.readUTF();
        index = in.readUTF();
        settings = readSettingsFromStream(in);
        timeout = readTimeValue(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            mappings.put(in.readUTF(), in.readUTF());
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            String type = in.readUTF();
            IndexMetaData.Custom customIndexMetaData = IndexMetaData.lookupFactorySafe(type).readFrom(in);
            customs.put(type, customIndexMetaData);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(cause);
        out.writeUTF(index);
        writeSettingsToStream(settings, out);
        timeout.writeTo(out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
        out.writeVInt(customs.size());
        for (Map.Entry<String, IndexMetaData.Custom> entry : customs.entrySet()) {
            out.writeUTF(entry.getKey());
            IndexMetaData.lookupFactorySafe(entry.getKey()).writeTo(entry.getValue(), out);
        }
    }
}