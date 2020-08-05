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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * A mapper for field aliases.
 *
 * A field alias has no concrete field mappings of its own, but instead points to another field by
 * its path. Once defined, an alias can be used in place of the concrete field name in search requests.
 */
public final class FieldAliasMapper extends Mapper {
    public static final String CONTENT_TYPE = "alias";

    public static class Names {
        public static final String PATH = "path";
    }

    private final String name;
    private final String path;

    public FieldAliasMapper(String simpleName,
                            String name,
                            String path) {
        super(simpleName);
        this.name = name;
        this.path = path;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    public String path() {
        return path;
    }

    @Override
    public Mapper merge(Mapper mergeWith) {
        if (!(mergeWith instanceof FieldAliasMapper)) {
            throw new IllegalArgumentException("Cannot merge a field alias mapping ["
                + name() + "] with a mapping that is not for a field alias.");
        }
        return mergeWith;
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(simpleName())
            .field("type", CONTENT_TYPE)
            .field(Names.PATH, path)
            .endObject();
    }

    @Override
    public void validate(MappingLookup mappers) {
        if (Objects.equals(this.path(), this.name())) {
            throw new MapperParsingException("Invalid [path] value [" + path + "] for field alias [" +
                name() + "]: an alias cannot refer to itself.");
        }
        if (mappers.fieldTypes().get(path) == null) {
            throw new MapperParsingException("Invalid [path] value [" + path + "] for field alias [" +
                name() + "]: an alias must refer to an existing field in the mappings.");
        }
        if (mappers.getMapper(path) instanceof FieldAliasMapper) {
            throw new MapperParsingException("Invalid [path] value [" + path + "] for field alias [" +
                name() + "]: an alias cannot refer to another alias.");
        }
        String aliasScope = mappers.getNestedScope(name);
        String pathScope = mappers.getNestedScope(path);

        if (!Objects.equals(aliasScope, pathScope)) {
            StringBuilder message = new StringBuilder("Invalid [path] value [" + path + "] for field alias [" +
                name + "]: an alias must have the same nested scope as its target. ");
            message.append(aliasScope == null
                ? "The alias is not nested"
                : "The alias's nested scope is [" + aliasScope + "]");
            message.append(", but ");
            message.append(pathScope == null
                ? "the target is not nested."
                : "the target's nested scope is [" + pathScope + "].");
            throw new IllegalArgumentException(message.toString());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            FieldAliasMapper.Builder builder = new FieldAliasMapper.Builder(name);
            Object pathField = node.remove(Names.PATH);
            String path = XContentMapValues.nodeStringValue(pathField, null);
            if (path == null) {
                throw new MapperParsingException("The [path] property must be specified for field [" + name + "].");
            }
            return builder.path(path);
        }
    }

    public static class Builder extends Mapper.Builder<FieldAliasMapper.Builder> {
        private String name;
        private String path;

        protected Builder(String name) {
            super(name);
            this.name = name;
        }

        public String name() {
            return this.name;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public FieldAliasMapper build(BuilderContext context) {
            String fullName = context.path().pathAsText(name);
            return new FieldAliasMapper(name, fullName, path);
        }
    }
}
