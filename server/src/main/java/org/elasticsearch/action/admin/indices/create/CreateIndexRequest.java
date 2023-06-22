/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;

/**
 * A request to create an index.
 * <p>
 * The index created can optionally be created with {@link #settings(org.elasticsearch.common.settings.Settings)}.
 *
 * @see org.elasticsearch.client.internal.IndicesAdminClient#create(CreateIndexRequest)
 * @see CreateIndexResponse
 */
public class CreateIndexRequest extends AcknowledgedRequest<CreateIndexRequest> implements IndicesRequest {

    public static final ParseField MAPPINGS = new ParseField("mappings");
    public static final ParseField SETTINGS = new ParseField("settings");
    public static final ParseField ALIASES = new ParseField("aliases");

    private String cause = "";

    private String index;

    private Settings settings = Settings.EMPTY;

    private String mappings = "{}";

    private final Set<Alias> aliases = new HashSet<>();

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private String origin = "";

    /**
     * Constructs a new request by deserializing an input
     * @param in the input from which to deserialize
     */
    public CreateIndexRequest(StreamInput in) throws IOException {
        super(in);
        cause = in.readString();
        index = in.readString();
        settings = readSettingsFromStream(in);
        if (in.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            int size = in.readVInt();
            assert size <= 1 : "Expected to read 0 or 1 mappings, but received " + size;
            if (size == 1) {
                String type = in.readString();
                if (MapperService.SINGLE_MAPPING_NAME.equals(type) == false) {
                    throw new IllegalArgumentException("Expected to receive mapping type of [_doc] but got [" + type + "]");
                }
                mappings = in.readString();
            }
        } else {
            mappings = in.readString();
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            aliases.add(new Alias(in));
        }
        waitForActiveShards = ActiveShardCount.readFrom(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_12_0)) {
            origin = in.readString();
        }
    }

    public CreateIndexRequest() {}

    /**
     * Constructs a request to create an index.
     *
     * @param index the name of the index
     */
    public CreateIndexRequest(String index) {
        this(index, Settings.EMPTY);
    }

    /**
     * Constructs a request to create an index.
     *
     * @param index the name of the index
     * @param settings the settings to apply to the index
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
        return new String[] { index };
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

    public String origin() {
        return origin;
    }

    public CreateIndexRequest origin(String origin) {
        this.origin = Objects.requireNonNull(origin);
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
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Set the mapping for this index
     *
     * The mapping should be in the form of a JSON string, with an outer _doc key
     * <pre>
     *     .mapping("{\"_doc\":{\"properties\": ... }}")
     * </pre>
     */
    public CreateIndexRequest mapping(String mapping) {
        this.mappings = mapping;
        return this;
    }

    /**
     * Set the mapping for this index
     *
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(XContentBuilder source) {
        return mapping(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Set the mapping for this index
     *
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(Map<String, ?> source) {
        return mapping(MapperService.SINGLE_MAPPING_NAME, source);
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public CreateIndexRequest simpleMapping(String... source) {
        mapping(PutMappingRequest.simpleMapping(source));
        return this;
    }

    private CreateIndexRequest mapping(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        Map<String, Object> mappingAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
        return mapping(MapperService.SINGLE_MAPPING_NAME, mappingAsMap);
    }

    private CreateIndexRequest mapping(String type, Map<String, ?> source) {
        // wrap it in a type map if its not
        if (source.size() != 1 || source.containsKey(type) == false) {
            source = Map.of(MapperService.SINGLE_MAPPING_NAME, source);
        } else if (MapperService.SINGLE_MAPPING_NAME.equals(type) == false) {
            // if it has a different type name, then unwrap and rewrap with _doc
            source = Map.of(MapperService.SINGLE_MAPPING_NAME, source.get(type));
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return mapping(Strings.toString(builder));
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * The cause for this index creation.
     */
    public CreateIndexRequest cause(String cause) {
        this.cause = cause;
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
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source)) {
            // move to the first alias
            parser.nextToken();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                alias(Alias.fromXContent(parser));
            }
            return this;
        } catch (IOException e) {
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

    public String mappings() {
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
        settings.writeTo(out);
        if (out.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            if ("{}".equals(mappings)) {
                out.writeVInt(0);
            } else {
                out.writeVInt(1);
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
                out.writeString(mappings);
            }
        } else {
            out.writeString(mappings);
        }
        out.writeCollection(aliases);
        waitForActiveShards.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_12_0)) {
            out.writeString(origin);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CreateIndexRequest that = (CreateIndexRequest) obj;
        return Objects.equals(cause, that.cause)
            && Objects.equals(index, that.index)
            && Objects.equals(settings, that.settings)
            && Objects.equals(mappings, that.mappings)
            && Objects.equals(aliases, that.aliases)
            && Objects.equals(waitForActiveShards, that.waitForActiveShards)
            && Objects.equals(origin, that.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cause, index, settings, mappings, aliases, waitForActiveShards, origin);
    }
}
