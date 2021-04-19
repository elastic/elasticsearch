/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class NestedObjectMapper extends ObjectMapper {

    public static final String CONTENT_TYPE = "nested";

    public static class Builder extends ObjectMapper.Builder {

        private Explicit<Boolean> includeInRoot = new Explicit<>(false, false);
        private Explicit<Boolean> includeInParent = new Explicit<>(false, false);

        public Builder(String name, Version indexCreatedVersion) {
            super(name, indexCreatedVersion);
        }

        void includeInRoot(boolean includeInRoot) {
            this.includeInRoot = new Explicit<>(includeInRoot, true);
        }

        void includeInParent(boolean includeInParent) {
            this.includeInParent = new Explicit<>(includeInParent, true);
        }

        @Override
        public NestedObjectMapper build(ContentPath contentPath) {
            contentPath.add(name);

            Map<String, Mapper> mappers = new HashMap<>();
            for (Mapper.Builder builder : mappersBuilders) {
                Mapper mapper = builder.build(contentPath);
                Mapper existing = mappers.get(mapper.simpleName());
                if (existing != null) {
                    mapper = existing.merge(mapper);
                }
                mappers.put(mapper.simpleName(), mapper);
            }
            contentPath.remove();

            return new NestedObjectMapper(name, contentPath.pathAsText(name), mappers, this);
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
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

    private final Explicit<Boolean> includeInRoot;
    private final Explicit<Boolean> includeInParent;
    private final String nestedTypePath;
    private final Query nestedTypeFilter;

    NestedObjectMapper(
        String name,
        String fullPath,
        Map<String, Mapper> mappers,
        Builder builder
    ) {
        super(name, fullPath, builder.enabled, builder.dynamic, mappers);
        if (builder.indexCreatedVersion.before(Version.V_8_0_0)) {
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
}
