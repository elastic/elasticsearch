/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformDestIndexSettings implements SimpleDiffable<TransformDestIndexSettings>, Writeable, ToXContentObject {

    public static final ParseField MAPPINGS = new ParseField("mappings");
    public static final ParseField SETTINGS = new ParseField("settings");
    public static final ParseField ALIASES = new ParseField("aliases");

    private static final ConstructingObjectParser<TransformDestIndexSettings, Void> STRICT_PARSER = createParser(false);

    private final Map<String, Object> mappings;
    private final Settings settings;
    private final Set<Alias> aliases;

    private static ConstructingObjectParser<TransformDestIndexSettings, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TransformDestIndexSettings, Void> PARSER = new ConstructingObjectParser<>(
            "transform_preview_generated_dest_index",
            lenient,
            args -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> mappings = (Map<String, Object>) args[0];
                Settings settings = (Settings) args[1];
                @SuppressWarnings("unchecked")
                Set<Alias> aliases = (Set<Alias>) args[2];

                return new TransformDestIndexSettings(mappings, settings, aliases);
            }
        );

        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), MAPPINGS);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            Set<Alias> aliases = new HashSet<>();
            while ((p.nextToken()) != XContentParser.Token.END_OBJECT) {
                aliases.add(Alias.fromXContent(p));
            }
            return aliases;
        }, ALIASES);

        return PARSER;
    }

    public TransformDestIndexSettings(Map<String, Object> mappings, Settings settings, Set<Alias> aliases) {
        this.mappings = mappings == null ? Collections.emptyMap() : Collections.unmodifiableMap(mappings);
        this.settings = settings == null ? Settings.EMPTY : settings;
        this.aliases = aliases == null ? Collections.emptySet() : Collections.unmodifiableSet(aliases);
    }

    public TransformDestIndexSettings(StreamInput in) throws IOException {
        mappings = in.readMap();
        settings = Settings.readSettingsFromStream(in);
        aliases = new HashSet<>(in.readList(Alias::new));
    }

    public Map<String, Object> getMappings() {
        return mappings;
    }

    public Settings getSettings() {
        return settings;
    }

    public Set<Alias> getAliases() {
        return aliases;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        // note: we write out the full object, even if parts are empty to gain visibility of options
        builder.startObject();
        builder.field(MAPPINGS.getPreferredName(), mappings);

        builder.startObject(SETTINGS.getPreferredName());
        settings.toXContent(builder, params);
        builder.endObject();

        builder.startObject(ALIASES.getPreferredName());
        for (Alias alias : aliases) {
            alias.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static TransformDestIndexSettings fromXContent(final XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(mappings);
        settings.writeTo(out);
        out.writeCollection(aliases);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        TransformDestIndexSettings other = (TransformDestIndexSettings) obj;
        return Objects.equals(other.mappings, mappings)
            && Objects.equals(other.settings, settings)
            && Objects.equals(other.aliases, aliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings, settings, aliases);
    }
}
