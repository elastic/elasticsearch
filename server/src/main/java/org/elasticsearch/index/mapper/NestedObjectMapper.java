/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * A Mapper for nested objects
 */
public class NestedObjectMapper extends ObjectMapper {

    public static final String CONTENT_TYPE = "nested";

    public static class Builder extends ObjectMapper.Builder {

        private Explicit<Boolean> includeInRoot = Explicit.IMPLICIT_FALSE;
        private Explicit<Boolean> includeInParent = Explicit.IMPLICIT_FALSE;
        private final IndexVersion indexCreatedVersion;

        public Builder(String name, IndexVersion indexCreatedVersion) {
            super(name, Explicit.IMPLICIT_TRUE);
            this.indexCreatedVersion = indexCreatedVersion;
        }

        Builder includeInRoot(boolean includeInRoot) {
            this.includeInRoot = Explicit.explicitBoolean(includeInRoot);
            return this;
        }

        Builder includeInParent(boolean includeInParent) {
            this.includeInParent = Explicit.explicitBoolean(includeInParent);
            return this;
        }

        @Override
        public NestedObjectMapper build(MapperBuilderContext context) {
            boolean parentIncludedInRoot = this.includeInRoot.value();
            if (context instanceof NestedMapperBuilderContext nc) {
                // we're already inside a nested mapper, so adjust our includes
                if (nc.parentIncludedInRoot && this.includeInParent.value()) {
                    this.includeInRoot = Explicit.IMPLICIT_FALSE;
                }
            } else {
                // this is a top-level nested mapper, so include_in_parent = include_in_root
                parentIncludedInRoot |= this.includeInParent.value();
                if (this.includeInParent.value()) {
                    this.includeInRoot = Explicit.IMPLICIT_FALSE;
                }
            }
            NestedMapperBuilderContext nestedContext = new NestedMapperBuilderContext(context.buildFullName(name), parentIncludedInRoot);
            return new NestedObjectMapper(name, context.buildFullName(name), buildMappers(nestedContext), this);
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            if (parseSubobjects(node).explicit()) {
                throw new MapperParsingException("Nested type [" + name + "] does not support [subobjects] parameter");
            }
            NestedObjectMapper.Builder builder = new NestedObjectMapper.Builder(name, parserContext.indexVersionCreated());
            parseNested(name, node, builder);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)) {
                    iterator.remove();
                }
            }
            return builder;
        }

        protected static void parseNested(String name, Map<String, Object> node, NestedObjectMapper.Builder builder) {
            Object fieldNode = node.get("include_in_parent");
            if (fieldNode != null) {
                boolean includeInParent = XContentMapValues.nodeBooleanValue(fieldNode, name + ".include_in_parent");
                builder.includeInParent(includeInParent);
                node.remove("include_in_parent");
            }
            fieldNode = node.get("include_in_root");
            if (fieldNode != null) {
                boolean includeInRoot = XContentMapValues.nodeBooleanValue(fieldNode, name + ".include_in_root");
                builder.includeInRoot(includeInRoot);
                node.remove("include_in_root");
            }
        }
    }

    private static class NestedMapperBuilderContext extends MapperBuilderContext {

        final boolean parentIncludedInRoot;

        NestedMapperBuilderContext(String path, boolean parentIncludedInRoot) {
            super(path, false);
            this.parentIncludedInRoot = parentIncludedInRoot;
        }

        @Override
        public MapperBuilderContext createChildContext(String name) {
            return new NestedMapperBuilderContext(buildFullName(name), parentIncludedInRoot);
        }
    }

    private Explicit<Boolean> includeInRoot;
    private Explicit<Boolean> includeInParent;
    private final String nestedTypePath;
    private final Query nestedTypeFilter;

    NestedObjectMapper(String name, String fullPath, Map<String, Mapper> mappers, Builder builder) {
        super(name, fullPath, builder.enabled, Explicit.IMPLICIT_TRUE, builder.dynamic, mappers);
        if (builder.indexCreatedVersion.before(IndexVersion.V_8_0_0)) {
            this.nestedTypePath = "__" + fullPath;
        } else {
            this.nestedTypePath = fullPath;
        }
        this.nestedTypeFilter = NestedPathFieldMapper.filter(builder.indexCreatedVersion, nestedTypePath);
        this.includeInParent = builder.includeInParent;
        this.includeInRoot = builder.includeInRoot;
    }

    public Query nestedTypeFilter() {
        return this.nestedTypeFilter;
    }

    public String nestedTypePath() {
        return this.nestedTypePath;
    }

    @Override
    public boolean isNested() {
        return true;
    }

    public boolean isIncludeInParent() {
        return this.includeInParent.value();
    }

    public void setIncludeInParent(boolean includeInParent) {
        this.includeInParent = Explicit.explicitBoolean(includeInParent);
    }

    public boolean isIncludeInRoot() {
        return this.includeInRoot.value();
    }

    public void setIncludeInRoot(boolean includeInRoot) {
        this.includeInRoot = Explicit.explicitBoolean(includeInRoot);
    }

    public Map<String, Mapper> getChildren() {
        return this.mappers;
    }

    @Override
    public ObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        NestedObjectMapper.Builder builder = new NestedObjectMapper.Builder(simpleName(), indexVersionCreated);
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        builder.includeInRoot = includeInRoot;
        builder.includeInParent = includeInParent;
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        builder.field("type", CONTENT_TYPE);
        if (includeInParent.explicit() && includeInParent.value()) {
            builder.field("include_in_parent", includeInParent.value());
        }
        if (includeInRoot.value()) {
            builder.field("include_in_root", includeInRoot.value());
        }
        if (dynamic != null) {
            builder.field("dynamic", dynamic.name().toLowerCase(Locale.ROOT));
        }
        if (isEnabled() != Defaults.ENABLED) {
            builder.field("enabled", enabled.value());
        }
        serializeMappers(builder, params);
        return builder.endObject();
    }

    @Override
    public ObjectMapper merge(Mapper mergeWith, MapperService.MergeReason reason, MapperBuilderContext parentBuilderContext) {
        if ((mergeWith instanceof NestedObjectMapper) == false) {
            throw new IllegalArgumentException("can't merge a non nested mapping [" + mergeWith.name() + "] with a nested mapping");
        }
        NestedObjectMapper mergeWithObject = (NestedObjectMapper) mergeWith;
        NestedObjectMapper toMerge = (NestedObjectMapper) clone();
        if (reason == MapperService.MergeReason.INDEX_TEMPLATE) {
            if (mergeWithObject.includeInParent.explicit()) {
                toMerge.includeInParent = mergeWithObject.includeInParent;
            }
            if (mergeWithObject.includeInRoot.explicit()) {
                toMerge.includeInRoot = mergeWithObject.includeInRoot;
            }
        } else {
            if (includeInParent.value() != mergeWithObject.includeInParent.value()) {
                throw new MapperException("the [include_in_parent] parameter can't be updated on a nested object mapping");
            }
            if (includeInRoot.value() != mergeWithObject.includeInRoot.value()) {
                throw new MapperException("the [include_in_root] parameter can't be updated on a nested object mapping");
            }
        }
        if (parentBuilderContext instanceof NestedMapperBuilderContext nc) {
            if (nc.parentIncludedInRoot && toMerge.includeInParent.value()) {
                toMerge.includeInRoot = Explicit.IMPLICIT_FALSE;
            }
        } else {
            if (toMerge.includeInParent.value()) {
                toMerge.includeInRoot = Explicit.IMPLICIT_FALSE;
            }
        }
        toMerge.doMerge(mergeWithObject, reason, parentBuilderContext);
        return toMerge;
    }

    @Override
    protected MapperBuilderContext createChildContext(MapperBuilderContext mapperBuilderContext, String name) {
        boolean parentIncludedInRoot = this.includeInRoot.value();
        if (mapperBuilderContext instanceof NestedMapperBuilderContext == false) {
            parentIncludedInRoot |= this.includeInParent.value();
        }
        return new NestedMapperBuilderContext(mapperBuilderContext.buildFullName(name), parentIncludedInRoot);
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        throw new IllegalArgumentException("field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source");
    }
}
