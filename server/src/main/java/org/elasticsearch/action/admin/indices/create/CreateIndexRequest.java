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

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

/**
 * A request to create an index. Best created with {@link org.elasticsearch.client.Requests#createIndexRequest(String)}.
 * <p>
 * The index created can optionally be created with {@link #settings(org.elasticsearch.common.settings.Settings)}.
 *
 * @see org.elasticsearch.client.IndicesAdminClient#create(CreateIndexRequest)
 * @see org.elasticsearch.client.Requests#createIndexRequest(String)
 * @see CreateIndexResponse
 */
public class CreateIndexRequest extends AcknowledgedRequest<CreateIndexRequest> implements IndicesRequest, ToXContentObject {

    public static final ParseField MAPPINGS = new ParseField("mappings");
    public static final ParseField SETTINGS = new ParseField("settings");
    public static final ParseField ALIASES = new ParseField("aliases");

    private String cause = "";

    private String index;

    private Settings settings = EMPTY_SETTINGS;

    private final Map<String, String> mappings = new HashMap<>();

    private final Set<Alias> aliases = new HashSet<>();

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    public CreateIndexRequest(StreamInput in) throws IOException {
        super(in);
        cause = in.readString();
        index = in.readString();
        settings = readSettingsFromStream(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            final String type = in.readString();
            String source = in.readString();
            mappings.put(type, source);
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            aliases.add(new Alias(in));
        }
        waitForActiveShards = ActiveShardCount.readFrom(in);
    }

    public CreateIndexRequest() {
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
     * The settings to create the index with.
     */
    public CreateIndexRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
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
     * The settings to create the index with (either json or yaml format)
     */
    public CreateIndexRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Allows to set the settings using a json builder.
     */
    public CreateIndexRequest settings(XContentBuilder builder) {
        settings(Strings.toString(builder), builder.contentType());
        return this;
    }

    /**
     * The settings to create the index with (either json/yaml/properties format)
     */
    public CreateIndexRequest settings(Map<String, ?> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(Strings.toString(builder), XContentType.JSON);
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
     * @param xContentType The content type of the source
     */
    public CreateIndexRequest mapping(String type, String source, XContentType xContentType) {
        return mapping(type, new BytesArray(source), xContentType);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     * @param xContentType the content type of the mapping source
     */
    private CreateIndexRequest mapping(String type, BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        Map<String, Object> mappingAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
        return mapping(type, mappingAsMap);
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
        return mapping(type, BytesReference.bytes(source), source.contentType());
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(String type, Map<String, ?> source) {
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = MapBuilder.<String, Object>newMapBuilder().put(type, source).map();
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            mappings.put(type, Strings.toString(builder));
            return this;
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
    public CreateIndexRequest aliases(Map<String, ?> source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return aliases(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(XContentBuilder source) {
        return aliases(BytesReference.bytes(source));
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
        // EMPTY is safe here because we never call namedObject
        try (XContentParser parser = XContentHelper
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source)) {
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
    public CreateIndexRequest source(String source, XContentType xContentType) {
        return source(new BytesArray(source), xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(XContentBuilder source) {
        return source(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        return source(new BytesArray(source, offset, length), xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        source(XContentHelper.convertToMap(source, false, xContentType).v2(), LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest source(Map<String, ?> source, DeprecationHandler deprecationHandler) {
        for (Map.Entry<String, ?> entry : source.entrySet()) {
            String name = entry.getKey();
            if (SETTINGS.match(name, deprecationHandler)) {
                if (entry.getValue() instanceof Map == false) {
                    throw new ElasticsearchParseException("key [settings] must be an object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (MAPPINGS.match(name, deprecationHandler)) {
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> entry1 : mappings.entrySet()) {
                    mapping(entry1.getKey(), (Map<String, Object>) entry1.getValue());
                }
            } else if (ALIASES.match(name, deprecationHandler)) {
                aliases((Map<String, Object>) entry.getValue());
            } else {
                throw new ElasticsearchParseException("unknown key [{}] for create index", name);
            }
        }
        return this;
    }

    public Map<String, String> mappings() {
        return this.mappings;
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that should be active for index creation to return.
     * Defaults to {@link ActiveShardCount#DEFAULT}, which will wait for one shard copy
     * (the primary) to become active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active before returning.
     * Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link CreateIndexResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public CreateIndexRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public CreateIndexRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(index);
        writeSettingsToStream(settings, out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeVInt(aliases.size());
        for (Alias alias : aliases) {
            alias.writeTo(out);
        }
        waitForActiveShards.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SETTINGS.getPreferredName());
        settings.toXContent(builder, params);
        builder.endObject();

        builder.startObject(MAPPINGS.getPreferredName());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            try (InputStream stream = new BytesArray(entry.getValue()).streamInput()) {
                builder.rawField(entry.getKey(), stream, XContentType.JSON);
            }
        }
        builder.endObject();

        builder.startObject(ALIASES.getPreferredName());
        for (Alias alias : aliases) {
            alias.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
