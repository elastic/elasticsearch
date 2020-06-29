/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import joptsimple.internal.Strings;
import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class ParametrizedFieldMapper extends FieldMapper {

    protected ParametrizedFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, new FieldType(), mappedFieldType, multiFields, copyTo);
    }

    public abstract ParametrizedFieldMapper.Builder getMergeBuilder();

    @Override
    public final ParametrizedFieldMapper merge(Mapper mergeWith) {

        if (mergeWith instanceof FieldMapper == false) {
            throw new IllegalArgumentException("mapper [" + name() + "] cannot be changed from type ["
                + contentType() + "] to [" + mergeWith.getClass().getSimpleName() + "]");
        }
        if (Objects.equals(this.getClass(), mergeWith.getClass()) == false) {
            throw new IllegalArgumentException("mapper [" + name() + "] cannot be changed from type ["
                + contentType() + "] to [" + ((FieldMapper) mergeWith).contentType() + "]");
        }

        ParametrizedFieldMapper.Builder builder = getMergeBuilder();
        Conflicts conflicts = new Conflicts(name());
        builder.merge((FieldMapper) mergeWith, conflicts);
        conflicts.check();
        return builder.build(new BuilderContext(Settings.EMPTY, parentPath(name())));
    }

    private static ContentPath parentPath(String name) {
        int endPos = name.lastIndexOf(".");
        if (endPos == -1) {
            return new ContentPath(0);
        }
        return new ContentPath(name.substring(endPos));
    }

    @Override
    protected final void mergeOptions(FieldMapper other, List<String> conflicts) {
        // TODO remove when everything is parametrized
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        builder.field("type", contentType());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        getMergeBuilder().toXContent(builder, includeDefaults);
        multiFields.toXContent(builder, params);
        copyTo.toXContent(builder, params);
        return builder.endObject();
    }

    public static final class Parameter<T> {

        private final T defaultValue;
        private final BiFunction<String, Object, T> parser;
        private final Function<FieldMapper, T> merger;
        public final String name;
        public final boolean updateable;
        private boolean acceptsNull = false;
        private T value;
        private boolean frozen = false;

        public Parameter(String name, boolean updateable, T defaultValue, BiFunction<String, Object, T> parser, Function<FieldMapper, T> merger) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.value = defaultValue;
            this.parser = parser;
            this.merger = merger;
            this.updateable = updateable;
        }

        public T getValue() {
            return value;
        }

        public Parameter<T> acceptsNull() {
            this.acceptsNull = true;
            return this;
        }

        public Parameter<T> init(FieldMapper toInit) {
            assert frozen == false;
            this.value = merger.apply(toInit);
            return this;
        }

        public Parameter<T> update(T value) {
            this.value = value;
            return this;
        }

        public Parameter<T> parse(String field, Object in) {
            assert frozen == false;
            this.value = parser.apply(field, in);
            return this;
        }

        public Parameter<T> merge(FieldMapper toMerge, Conflicts conflicts) {
            T value = merger.apply(toMerge);
            if (frozen && Objects.equals(this.value, value) == false) {
                conflicts.addConflict(name, this.value.toString(), value.toString());
            } else {
                this.value = value;
            }
            return this;
        }

        public Parameter<T> freeze() {
            this.frozen = true;
            return this;
        }

        public XContentBuilder toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
            if (includeDefaults || (Objects.equals(defaultValue, value) == false)) {
                builder.field(name, value);
            }
            return builder;
        }

        public static Parameter<Boolean> boolParam(String name, boolean updateable, Function<FieldMapper, Boolean> merger, boolean defaultValue) {
            return new Parameter<>(name, updateable, defaultValue, (n, o) -> XContentMapValues.nodeBooleanValue(o), merger);
        }

        public static Parameter<Float> floatParam(String name, boolean updateable, Function<FieldMapper, Float> merger, float defaultValue) {
            return new Parameter<>(name, updateable, defaultValue, (n, o) -> XContentMapValues.nodeFloatValue(o), merger);
        }

        public static Parameter<String> stringParam(String name, boolean updateable, Function<FieldMapper, String> merger, String defaultValue) {
            return new Parameter<>(name, updateable, defaultValue, (n, o) -> XContentMapValues.nodeStringValue(o, defaultValue), merger);
        }
    }

    public static final class Conflicts {

        private final String mapperName;
        private final List<String> conflicts = new ArrayList<>();

        public Conflicts(String mapperName) {
            this.mapperName = mapperName;
        }

        public void addConflict(String parameter, String existing, String toMerge) {
            conflicts.add("Cannot update parameter [" + parameter + "] from [" + existing + "] to [" + toMerge + "]");
        }

        public void check() {
            if (conflicts.isEmpty()) {
                return;
            }
            String message = "Mapper for [" + mapperName + "] conflicts with existing mapper:\n\t"
                + Strings.join(conflicts, "\n\t");
            throw new IllegalArgumentException(message);
        }

    }

    public abstract static class Builder extends Mapper.Builder<Builder> {

        protected final MultiFields.Builder multiFieldsBuilder = new MultiFields.Builder();
        protected CopyTo.Builder copyTo = new CopyTo.Builder();

        protected final Parameter<Map<String, String>> meta
            = new Parameter<>("meta", true, Collections.emptyMap(), TypeParsers::parseMeta, m -> m.fieldType().meta());

        protected Builder(String name) {
            super(name);
        }

        protected Builder init(FieldMapper base) {
            for (Parameter<?> param : getParameters()) {
                param.init(base);
            }
            for (Mapper subField : base.multiFields) {
                multiFieldsBuilder.add(subField);
            }
            return this;
        }

        public final void merge(FieldMapper in, Conflicts conflicts) {
            for (Parameter<?> param : getParameters()) {
                if (param.updateable == false) {
                    param.freeze();
                }
                param.merge(in, conflicts);
            }
            for (Mapper newSubField : in.multiFields) {
                multiFieldsBuilder.update(newSubField, parentPath(newSubField.name()));
            }
            this.copyTo.reset(in.copyTo);
        }

        protected abstract List<Parameter<?>> getParameters();

        @Override
        public abstract ParametrizedFieldMapper build(BuilderContext context);

        protected String buildFullName(BuilderContext context) {
            return context.path().pathAsText(name);
        }

        public final XContentBuilder toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
            for (Parameter<?> parameter : getParameters()) {
                parameter.toXContent(builder, includeDefaults);
            }
            return builder;
        }

        public final void parse(String name, TypeParser.ParserContext parserContext, Map<String, Object> fieldNode) {
            Map<String, Parameter<?>> paramsMap = new HashMap<>();
            for (Parameter<?> param : getParameters()) {
                paramsMap.put(param.name, param);
            }
            String type = (String) fieldNode.remove("type");
            for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                final String propName = entry.getKey();
                final Object propNode = entry.getValue();
                if (Objects.equals("fields", propName)) {
                    TypeParsers.parseMultiField(multiFieldsBuilder::add, name, parserContext, propName, propNode);
                    iterator.remove();
                    continue;
                }
                if (Objects.equals("copy_to", propName)) {
                    TypeParsers.parseCopyFields(propNode).forEach(copyTo::add);
                    iterator.remove();
                    continue;
                }
                Parameter<?> parameter = paramsMap.get(propName);
                if (parameter == null) {
                    throw new MapperParsingException("unknown parameter [" + propName
                        + "] on mapper [" + name + "] of type [" + type + "]");
                }
                if (propNode == null && parameter.acceptsNull == false) {
                    throw new MapperParsingException("[" + propName + "] on mapper [" + name
                        + "] of type [" + type + "] must not have a [null] value");
                }
                parameter.parse(name, propNode);
                iterator.remove();
            }
        }
    }
}
