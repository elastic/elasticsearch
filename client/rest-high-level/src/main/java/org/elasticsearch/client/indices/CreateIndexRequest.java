/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A request to create an index.
 */
public class CreateIndexRequest extends TimedRequest implements Validatable, ToXContentObject {
    static final ParseField MAPPINGS = new ParseField("mappings");
    static final ParseField SETTINGS = new ParseField("settings");
    static final ParseField ALIASES = new ParseField("aliases");

    private final String index;
    private Settings settings = Settings.EMPTY;

    private BytesReference mappings;
    private XContentType mappingsXContentType;

    private final Set<Alias> aliases = new HashSet<>();

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    /**
     * Constructs a new request to create an index with the specified name.
     */
    public CreateIndexRequest(String index) {
        if (index == null) {
            throw new IllegalArgumentException("The index name cannot be null.");
        }
        this.index = index;
    }

    /**
     * The name of the index to create.
     */
    public String index() {
        return index;
    }

    /**
     * The settings to create the index with.
     */
    public Settings settings() {
        return settings;
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

    public BytesReference mappings() {
        return mappings;
    }

    public XContentType mappingsXContentType() {
        return mappingsXContentType;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * Note that the definition should *not* be nested under a type name.
     *
     * @param source The mapping source
     * @param xContentType The content type of the source
     */
    public CreateIndexRequest mapping(String source, XContentType xContentType) {
        return mapping(new BytesArray(source), xContentType);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * Note that the definition should *not* be nested under a type name.
     *
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(XContentBuilder source) {
        return mapping(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * Note that the definition should *not* be nested under a type name.
     *
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(Map<String, ?> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            return mapping(BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * Note that the definition should *not* be nested under a type name.
     *
     * @param source The mapping source
     * @param xContentType the content type of the mapping source
     */
    public CreateIndexRequest mapping(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        mappings = source;
        mappingsXContentType = xContentType;
        return this;
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(Map<String, ?> source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return aliases(BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(XContentBuilder source) {
        return aliases(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(String source, XContentType contentType) {
        return aliases(new BytesArray(source), contentType);
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(BytesReference source, XContentType contentType) {
        // EMPTY is safe here because we never call namedObject
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                contentType
            )
        ) {
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
     * Adds aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(Collection<Alias> aliases) {
        this.aliases.addAll(aliases);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * Note that the mapping definition should *not* be nested under a type name.
     */
    public CreateIndexRequest source(String source, XContentType xContentType) {
        return source(new BytesArray(source), xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * Note that the mapping definition should *not* be nested under a type name.
     */
    public CreateIndexRequest source(XContentBuilder source) {
        return source(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * Note that the mapping definition should *not* be nested under a type name.
     */
    public CreateIndexRequest source(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        source(XContentHelper.convertToMap(source, false, xContentType).v2());
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * Note that the mapping definition should *not* be nested under a type name.
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest source(Map<String, ?> source) {
        DeprecationHandler deprecationHandler = DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
        for (Map.Entry<String, ?> entry : source.entrySet()) {
            String name = entry.getKey();
            if (SETTINGS.match(name, deprecationHandler)) {
                settings((Map<String, Object>) entry.getValue());
            } else if (MAPPINGS.match(name, deprecationHandler)) {
                mapping((Map<String, Object>) entry.getValue());
            } else if (ALIASES.match(name, deprecationHandler)) {
                aliases((Map<String, Object>) entry.getValue());
            }
        }
        return this;
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

        if (mappings != null) {
            try (InputStream stream = mappings.streamInput()) {
                builder.rawField(MAPPINGS.getPreferredName(), stream, mappingsXContentType);
            }
        }

        builder.startObject(ALIASES.getPreferredName());
        for (Alias alias : aliases) {
            alias.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
