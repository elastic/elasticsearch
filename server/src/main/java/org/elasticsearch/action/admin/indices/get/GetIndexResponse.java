/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.rest.BaseRestHandler.DEFAULT_INCLUDE_TYPE_NAME_POLICY;
import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;

/**
 * A response for a get index action.
 */
public class GetIndexResponse extends ActionResponse implements ChunkedToXContentObject {

    private Map<String, MappingMetadata> mappings = Map.of();
    private Map<String, List<AliasMetadata>> aliases = Map.of();
    private Map<String, Settings> settings = Map.of();
    private Map<String, Settings> defaultSettings = Map.of();
    private Map<String, String> dataStreams = Map.of();
    private final String[] indices;

    public GetIndexResponse(
        String[] indices,
        Map<String, MappingMetadata> mappings,
        Map<String, List<AliasMetadata>> aliases,
        Map<String, Settings> settings,
        Map<String, Settings> defaultSettings,
        Map<String, String> dataStreams
    ) {
        this.indices = indices;
        // to have deterministic order
        Arrays.sort(indices);
        if (mappings != null) {
            this.mappings = mappings;
        }
        if (aliases != null) {
            this.aliases = aliases;
        }
        if (settings != null) {
            this.settings = settings;
        }
        if (defaultSettings != null) {
            this.defaultSettings = defaultSettings;
        }
        if (dataStreams != null) {
            this.dataStreams = dataStreams;
        }
    }

    GetIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        mappings = in.readImmutableOpenMap(StreamInput::readString, in.getTransportVersion().before(TransportVersions.V_8_0_0) ? i -> {
            int numMappings = i.readVInt();
            assert numMappings == 0 || numMappings == 1 : "Expected 0 or 1 mappings but got " + numMappings;
            if (numMappings == 1) {
                String type = i.readString();
                assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected [_doc] but got [" + type + "]";
                return new MappingMetadata(i);
            } else {
                return MappingMetadata.EMPTY_MAPPINGS;
            }
        } : i -> i.readBoolean() ? new MappingMetadata(i) : MappingMetadata.EMPTY_MAPPINGS);

        aliases = in.readImmutableOpenMap(StreamInput::readString, i -> i.readCollectionAsList(AliasMetadata::new));
        settings = in.readImmutableOpenMap(StreamInput::readString, Settings::readSettingsFromStream);
        defaultSettings = in.readImmutableOpenMap(StreamInput::readString, Settings::readSettingsFromStream);
        dataStreams = in.readImmutableOpenMap(StreamInput::readString, StreamInput::readOptionalString);
    }

    public String[] indices() {
        return indices;
    }

    public String[] getIndices() {
        return indices();
    }

    public Map<String, MappingMetadata> mappings() {
        return mappings;
    }

    public Map<String, MappingMetadata> getMappings() {
        return mappings();
    }

    public Map<String, List<AliasMetadata>> aliases() {
        return aliases;
    }

    public Map<String, List<AliasMetadata>> getAliases() {
        return aliases();
    }

    public Map<String, Settings> settings() {
        return settings;
    }

    public Map<String, String> dataStreams() {
        return dataStreams;
    }

    public Map<String, String> getDataStreams() {
        return dataStreams();
    }

    /**
     * If the originating {@link GetIndexRequest} object was configured to include
     * defaults, this will contain a mapping of index name to {@link Settings} objects.
     * The returned {@link Settings} objects will contain only those settings taking
     * effect as defaults.  Any settings explicitly set on the index will be available
     * via {@link #settings()}.
     * See also {@link GetIndexRequest#includeDefaults(boolean)}
     */
    public Map<String, Settings> defaultSettings() {
        return defaultSettings;
    }

    public Map<String, Settings> getSettings() {
        return settings();
    }

    /**
     * Returns the string value for the specified index and setting.  If the includeDefaults flag was not set or set to
     * false on the {@link GetIndexRequest}, this method will only return a value where the setting was explicitly set
     * on the index.  If the includeDefaults flag was set to true on the {@link GetIndexRequest}, this method will fall
     * back to return the default value if the setting was not explicitly set.
     */
    public String getSetting(String index, String setting) {
        Settings indexSettings = settings.get(index);
        if (setting != null) {
            if (indexSettings != null && indexSettings.hasValue(setting)) {
                return indexSettings.get(setting);
            } else {
                Settings defaultIndexSettings = defaultSettings.get(index);
                if (defaultIndexSettings != null) {
                    return defaultIndexSettings.get(setting);
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        MappingMetadata.writeMappingMetadata(out, mappings);
        out.writeMap(aliases, StreamOutput::writeCollection);
        out.writeMap(settings, StreamOutput::writeWriteable);
        out.writeMap(defaultSettings, StreamOutput::writeWriteable);
        out.writeMap(dataStreams, StreamOutput::writeOptionalString);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.startObject()),
            Iterators.map(Iterators.forArray(indices), index -> (builder, params) -> {
                builder.startObject(index);

                builder.startObject("aliases");
                List<AliasMetadata> indexAliases = aliases.get(index);
                if (indexAliases != null) {
                    for (final AliasMetadata alias : indexAliases) {
                        AliasMetadata.Builder.toXContent(alias, builder, params);
                    }
                }
                builder.endObject();

                MappingMetadata indexMappings = mappings.get(index);
                if (indexMappings == null) {
                    builder.startObject("mappings").endObject();
                } else {
                    if (builder.getRestApiVersion() == RestApiVersion.V_7
                        && params.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY)) {
                        builder.startObject("mappings");
                        builder.field(MapperService.SINGLE_MAPPING_NAME, indexMappings.sourceAsMap());
                        builder.endObject();
                    } else {
                        builder.field("mappings", indexMappings.sourceAsMap());
                    }
                }

                builder.startObject("settings");
                Settings indexSettings = settings.get(index);
                if (indexSettings != null) {
                    indexSettings.toXContent(builder, params);
                }
                builder.endObject();

                Settings defaultIndexSettings = defaultSettings.get(index);
                if (defaultIndexSettings != null && defaultIndexSettings.isEmpty() == false) {
                    builder.startObject("defaults");
                    defaultIndexSettings.toXContent(builder, params);
                    builder.endObject();
                }

                String dataStream = dataStreams.get(index);
                if (dataStream != null) {
                    builder.field("data_stream", dataStream);
                }

                return builder.endObject();
            }),
            Iterators.single((builder, params) -> builder.endObject())
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetIndexResponse that = (GetIndexResponse) o;
        return Arrays.equals(indices, that.indices)
            && Objects.equals(aliases, that.aliases)
            && Objects.equals(mappings, that.mappings)
            && Objects.equals(settings, that.settings)
            && Objects.equals(defaultSettings, that.defaultSettings)
            && Objects.equals(dataStreams, that.dataStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), aliases, mappings, settings, defaultSettings, dataStreams);
    }
}
