/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;

/**
 * A request to create an index template.
 */
public class PutIndexTemplateRequest extends MasterNodeRequest<PutIndexTemplateRequest> implements IndicesRequest {

    private String name;

    private String cause = "";

    private List<String> indexPatterns;

    private int order;

    private boolean create;

    private Settings settings = Settings.EMPTY;

    @Nullable
    private String mappings;

    private final Set<Alias> aliases = new HashSet<>();

    private Integer version;

    public PutIndexTemplateRequest(StreamInput in) throws IOException {
        super(in);
        cause = in.readString();
        name = in.readString();
        indexPatterns = in.readStringList();
        order = in.readInt();
        create = in.readBoolean();
        settings = readSettingsFromStream(in);
        if (in.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                in.readString();    // type - cannot assert on _doc because 7x allows arbitrary type names
                this.mappings = in.readString();
            }
        } else {
            this.mappings = in.readOptionalString();
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            aliases.add(new Alias(in));
        }
        version = in.readOptionalVInt();
    }

    public PutIndexTemplateRequest() {}

    /**
     * Constructs a new put index template request with the provided name.
     */
    public PutIndexTemplateRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (indexPatterns == null || indexPatterns.size() == 0) {
            validationException = addValidationError("index patterns are missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the index template.
     */
    public PutIndexTemplateRequest name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The name of the index template.
     */
    public String name() {
        return this.name;
    }

    public PutIndexTemplateRequest patterns(List<String> indexPatterns) {
        this.indexPatterns = indexPatterns;
        return this;
    }

    public List<String> patterns() {
        return this.indexPatterns;
    }

    public PutIndexTemplateRequest order(int order) {
        this.order = order;
        return this;
    }

    public int order() {
        return this.order;
    }

    public PutIndexTemplateRequest version(Integer version) {
        this.version = version;
        return this;
    }

    public Integer version() {
        return this.version;
    }

    /**
     * Set to {@code true} to force only creation, not an update of an index template. If it already
     * exists, it will fail with an {@link IllegalArgumentException}.
     */
    public PutIndexTemplateRequest create(boolean create) {
        this.create = create;
        return this;
    }

    public boolean create() {
        return create;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to create the index template with (either json/yaml format).
     */
    public PutIndexTemplateRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * The settings to create the index template with (either json or yaml format).
     */
    public PutIndexTemplateRequest settings(Map<String, Object> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    public Settings settings() {
        return this.settings;
    }

    /**
     * The cause for this index template creation.
     */
    public PutIndexTemplateRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    public String cause() {
        return this.cause;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @param xContentType The type of content contained within the source
     */
    public PutIndexTemplateRequest mapping(String source, XContentType xContentType) {
        return mapping(new BytesArray(source), xContentType);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(XContentBuilder source) {
        return mapping(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @param xContentType the source content type
     */
    public PutIndexTemplateRequest mapping(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        Map<String, Object> mappingAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
        return mapping(mappingAsMap);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(Map<String, Object> source) {
        if (source.size() != 1 || source.containsKey(MapperService.SINGLE_MAPPING_NAME) == false) {
            source = Map.of(MapperService.SINGLE_MAPPING_NAME, source);
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            mappings = Strings.toString(builder);
            return this;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public PutIndexTemplateRequest mapping(String... source) {
        mapping(PutMappingRequest.simpleMapping(source));
        return this;
    }

    public String mappings() {
        return this.mappings;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(XContentBuilder templateBuilder) {
        try {
            return source(BytesReference.bytes(templateBuilder), templateBuilder.contentType());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to build json for template request", e);
        }
    }

    /**
     * The template source definition.
     */
    @SuppressWarnings("unchecked")
    public PutIndexTemplateRequest source(Map<String, Object> templateSource) {
        Map<String, Object> source = templateSource;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("index_patterns")) {
                if (entry.getValue() instanceof String) {
                    patterns(Collections.singletonList((String) entry.getValue()));
                } else if (entry.getValue() instanceof List) {
                    List<String> elements = ((List<?>) entry.getValue()).stream().map(Object::toString).toList();
                    patterns(elements);
                } else {
                    throw new IllegalArgumentException("Malformed [index_patterns] value, should be a string or a list of strings");
                }
            } else if (name.equals("order")) {
                order(XContentMapValues.nodeIntegerValue(entry.getValue(), order()));
            } else if ("version".equals(name)) {
                if ((entry.getValue() instanceof Integer) == false) {
                    throw new IllegalArgumentException("Malformed [version] value, should be an integer");
                }
                version((Integer) entry.getValue());
            } else if (name.equals("settings")) {
                if ((entry.getValue() instanceof Map) == false) {
                    throw new IllegalArgumentException("Malformed [settings] section, should include an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("mappings")) {
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> entry1 : mappings.entrySet()) {
                    if ((entry1.getValue() instanceof Map) == false) {
                        throw new IllegalArgumentException(
                            "Malformed [mappings] section for type ["
                                + entry1.getKey()
                                + "], should include an inner object describing the mapping"
                        );
                    }
                    mapping((Map<String, Object>) entry1.getValue());
                }
            } else if (name.equals("aliases")) {
                aliases((Map<String, Object>) entry.getValue());
            } else {
                throw new ElasticsearchParseException("unknown key [{}] in the template ", name);
            }
        }
        return this;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(String templateSource, XContentType xContentType) {
        return source(XContentHelper.convertToMap(xContentType.xContent(), templateSource, true));
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        return source(new BytesArray(source, offset, length), xContentType);
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(BytesReference source, XContentType xContentType) {
        return source(XContentHelper.convertToMap(source, true, xContentType).v2());
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(Map<String, ?> source) {
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
    public PutIndexTemplateRequest aliases(String source) {
        return aliases(new BytesArray(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(BytesReference source) {
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
     * Adds an alias that will be added when the index gets created.
     *
     * @param alias   The metadata for the new alias
     * @return  the index template creation request
     */
    public PutIndexTemplateRequest alias(Alias alias) {
        aliases.add(alias);
        return this;
    }

    @Override
    public String[] indices() {
        return indexPatterns.toArray(new String[indexPatterns.size()]);
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictExpand();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(name);
        out.writeStringCollection(indexPatterns);
        out.writeInt(order);
        out.writeBoolean(create);
        settings.writeTo(out);
        if (out.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            out.writeVInt(mappings == null ? 0 : 1);
            if (mappings != null) {
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
                out.writeString(mappings);
            }
        } else {
            out.writeOptionalString(mappings);
        }
        out.writeCollection(aliases);
        out.writeOptionalVInt(version);
    }
}
