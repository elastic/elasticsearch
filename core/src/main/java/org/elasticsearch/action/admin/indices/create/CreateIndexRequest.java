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

package org.elasticsearch.action.admin.indices.create;

import java.nio.charset.StandardCharsets;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

/**
 * A request to create an index. Best created with {@link org.elasticsearch.client.Requests#createIndexRequest(String)}.
 * <p/>
 * <p>The index created can optionally be created with {@link #settings(org.elasticsearch.common.settings.Settings)}.
 *
 * @see org.elasticsearch.client.IndicesAdminClient#create(CreateIndexRequest)
 * @see org.elasticsearch.client.Requests#createIndexRequest(String)
 * @see CreateIndexResponse
 */
public class CreateIndexRequest extends AcknowledgedRequest<CreateIndexRequest> implements IndicesRequest {

    private String cause = "";

    private String index;

    private Settings settings = EMPTY_SETTINGS;

    private final Map<String, String> mappings = new HashMap<>();

    private final Set<Alias> aliases = new HashSet<>();

    private final Map<String, IndexMetaData.Custom> customs = new HashMap<>();

    private boolean updateAllTypes = false;

    public CreateIndexRequest() {
    }

    /**
     * Constructs a new request to create an index that was triggered by a different request,
     * provided as an argument so that its headers and context can be copied to the new request.
     */
    public CreateIndexRequest(ActionRequest request) {
        super(request);
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

    @Override
    public String[] indices() {
        return new String[]{index};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    /**
     * The index name to create.
     */
    public String index() {
        return index;
    }

    public CreateIndexRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public Settings settings() {
        return settings;
    }

    /**
     * The cause for this index creation.
     */
    public String cause() {
        return cause;
    }

    /**
     * A simplified version of settings that takes key value pairs settings.
     */
    public CreateIndexRequest settings(Object... settings) {
        this.settings = Settings.builder().put(settings).build();
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to create the index with (either json/yaml/properties format)
     */
    public CreateIndexRequest settings(String source) {
        this.settings = Settings.settingsBuilder().loadFromSource(source).build();
        return this;
    }

    /**
     * Allows to set the settings using a json builder.
     */
    public CreateIndexRequest settings(XContentBuilder builder) {
        try {
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate json settings from builder", e);
        }
        return this;
    }

    /**
     * The settings to create the index with (either json/yaml/properties format)
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest settings(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
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
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
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
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
        try {
            mappings.put(type, source.string());
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to build json for mapping request", e);
        }
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest mapping(String type, Map source) {
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = MapBuilder.<String, Object>newMapBuilder().put(type, source).map();
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            return mapping(type, builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public CreateIndexRequest mapping(String type, Object... source) {
        mapping(type, PutMappingRequest.buildFromSimplifiedDef(type, source));
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest aliases(Map source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return aliases(builder.bytes());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(XContentBuilder source) {
        return aliases(source.bytes());
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(String source) {
        return aliases(new BytesArray(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(BytesReference source) {
        try {
            XContentParser parser = XContentHelper.createParser(source);
            //move to the first alias
            parser.nextToken();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                alias(Alias.fromXContent(parser));
            }
            return this;
        } catch(IOException e) {
            throw new ElasticsearchParseException("Failed to parse aliases", e);
        }
    }

    /**
     * Adds an alias that will be associated with the index when it gets created
     */
    public CreateIndexRequest alias(Alias alias) {
        this.aliases.add(alias);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(String source) {
        return source(source.getBytes(StandardCharsets.UTF_8));
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

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(byte[] source, int offset, int length) {
        return source(new BytesArray(source, offset, length));
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(BytesReference source) {
        XContentType xContentType = XContentFactory.xContentType(source);
        if (xContentType != null) {
            try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(source)) {
                source(parser.map());
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse source for create index", e);
            }
        } else {
            settings(new String(source.toBytes(), StandardCharsets.UTF_8));
        }
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest source(Map<String, ?> source) {
        boolean found = false;
        for (Map.Entry<String, ?> entry : source.entrySet()) {
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
            } else if (name.equals("aliases")) {
                found = true;
                aliases((Map<String, Object>) entry.getValue());
            } else {
                // maybe custom?
                IndexMetaData.Custom proto = IndexMetaData.lookupPrototype(name);
                if (proto != null) {
                    found = true;
                    try {
                        customs.put(name, proto.fromMap((Map<String, Object>) entry.getValue()));
                    } catch (IOException e) {
                        throw new ElasticsearchParseException("failed to parse custom metadata for [{}]", name);
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

    public Map<String, String> mappings() {
        return this.mappings;
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Adds custom metadata to the index to be created.
     */
    public CreateIndexRequest custom(IndexMetaData.Custom custom) {
        customs.put(custom.type(), custom);
        return this;
    }

    public Map<String, IndexMetaData.Custom> customs() {
        return this.customs;
    }

    /** True if all fields that span multiple types should be updated, false otherwise */
    public boolean updateAllTypes() {
        return updateAllTypes;
    }

    /** See {@link #updateAllTypes()} */
    public CreateIndexRequest updateAllTypes(boolean updateAllTypes) {
        this.updateAllTypes = updateAllTypes;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cause = in.readString();
        index = in.readString();
        settings = readSettingsFromStream(in);
        readTimeout(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            mappings.put(in.readString(), in.readString());
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            String type = in.readString();
            IndexMetaData.Custom customIndexMetaData = IndexMetaData.lookupPrototypeSafe(type).readFrom(in);
            customs.put(type, customIndexMetaData);
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            aliases.add(Alias.read(in));
        }
        updateAllTypes = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(index);
        writeSettingsToStream(settings, out);
        writeTimeout(out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeVInt(customs.size());
        for (Map.Entry<String, IndexMetaData.Custom> entry : customs.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
        out.writeVInt(aliases.size());
        for (Alias alias : aliases) {
            alias.writeTo(out);
        }
        out.writeBoolean(updateAllTypes);
    }
}
