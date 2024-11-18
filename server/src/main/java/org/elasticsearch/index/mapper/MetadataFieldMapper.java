/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * A mapper for a builtin field containing metadata about a document.
 */
public abstract class MetadataFieldMapper extends FieldMapper {

    public interface TypeParser {

        MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException;

        /**
         * Get the default {@link MetadataFieldMapper} to use, if nothing had to be parsed.
         *
         * @param parserContext context that may be useful to build the field like analyzers
         */
        MetadataFieldMapper getDefault(MappingParserContext parserContext);
    }

    /**
     * Declares an updateable boolean parameter for a metadata field
     *
     * We need to distinguish between explicit configuration and default value for metadata
     * fields, because mapping updates will carry over the previous metadata values if a
     * metadata field is not explicitly declared in the update.  A standard boolean
     * parameter explicitly configured with a default value will not be serialized (as
     * we do not serialize default parameters for mapping updates), and as such will be
     * ignored by the update merge.  Instead, we use an {@link Explicit} object that
     * will serialize its value if it has been configured, no matter what the value is.
     */
    public static Parameter<Explicit<Boolean>> updateableBoolParam(
        String name,
        Function<FieldMapper, Explicit<Boolean>> initializer,
        boolean defaultValue
    ) {
        return new Parameter<>(
            name,
            true,
            defaultValue ? () -> Explicit.IMPLICIT_TRUE : () -> Explicit.IMPLICIT_FALSE,
            (n, c, o) -> Explicit.explicitBoolean(XContentMapValues.nodeBooleanValue(o)),
            initializer,
            (b, n, v) -> b.field(n, v.value()),
            v -> Boolean.toString(v.value())
        );
    }

    /**
     * A type parser for an unconfigurable metadata field.
     */
    public static class FixedTypeParser implements TypeParser {

        final Function<MappingParserContext, MetadataFieldMapper> mapperParser;

        public FixedTypeParser(Function<MappingParserContext, MetadataFieldMapper> mapperParser) {
            this.mapperParser = mapperParser;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(name + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(MappingParserContext parserContext) {
            return mapperParser.apply(parserContext);
        }
    }

    public static class ConfigurableTypeParser implements TypeParser {

        final Function<MappingParserContext, MetadataFieldMapper> defaultMapperParser;
        final Function<MappingParserContext, Builder> builderFunction;

        public ConfigurableTypeParser(
            Function<MappingParserContext, MetadataFieldMapper> defaultMapperParser,
            Function<MappingParserContext, Builder> builderFunction
        ) {
            this.defaultMapperParser = defaultMapperParser;
            this.builderFunction = builderFunction;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext) throws MapperParsingException {
            Builder builder = builderFunction.apply(parserContext);
            builder.parseMetadataField(name, parserContext, node);
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappingParserContext parserContext) {
            return defaultMapperParser.apply(parserContext);
        }
    }

    public abstract static class Builder extends FieldMapper.Builder {

        protected Builder(String name) {
            super(name);
        }

        boolean isConfigured() {
            for (Parameter<?> param : getParameters()) {
                if (param.isConfigured()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public final MetadataFieldMapper build(MapperBuilderContext context) {
            return build();
        }

        public final void parseMetadataField(String name, MappingParserContext parserContext, Map<String, Object> fieldNode) {
            final Parameter<?>[] params = getParameters();
            Map<String, Parameter<?>> paramsMap = Maps.newHashMapWithExpectedSize(params.length);
            for (Parameter<?> param : params) {
                paramsMap.put(param.name, param);
            }
            for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                final String propName = entry.getKey();
                final Object propNode = entry.getValue();
                Parameter<?> parameter = paramsMap.get(propName);
                if (parameter == null) {
                    throw new MapperParsingException("unknown parameter [" + propName + "] on metadata field [" + name + "]");
                }
                parameter.parse(name, parserContext, propNode);
                iterator.remove();
            }
            validate();
        }

        public abstract MetadataFieldMapper build();
    }

    protected MetadataFieldMapper(MappedFieldType mappedFieldType) {
        super(mappedFieldType.name(), mappedFieldType, BuilderParams.empty());
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return null;    // by default, things can't be configured so we have no builder
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        MetadataFieldMapper.Builder mergeBuilder = (MetadataFieldMapper.Builder) getMergeBuilder();
        if (mergeBuilder == null || mergeBuilder.isConfigured() == false) {
            return builder;
        }
        builder.startObject(leafName());
        getMergeBuilder().toXContent(builder, params);
        return builder.endObject();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "Field [" + fullPath() + "] is a metadata field and cannot be added inside a document. Use the index API request parameters."
        );
    }

    /**
     * Called before {@link FieldMapper#parse(DocumentParserContext)} on the {@link RootObjectMapper}.
     */
    public void preParse(DocumentParserContext context) throws IOException {
        // do nothing
    }

    /**
     * Called after {@link FieldMapper#parse(DocumentParserContext)} on the {@link RootObjectMapper}.
     */
    public void postParse(DocumentParserContext context) throws IOException {
        // do nothing
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(SourceLoader.SyntheticFieldLoader.NOTHING);
    }
}
