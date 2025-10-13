/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class NonDynamicFieldMapperTests extends NonDynamicFieldMapperTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(NonDynamicFieldPlugin.class);
    }

    protected String getTypeName() {
        return NonDynamicFieldMapper.NAME;
    }

    protected String getMapping() {
        return String.format(Locale.ROOT, """
            "type": "%s"
            """, NonDynamicFieldMapper.NAME);
    }

    public static class NonDynamicFieldPlugin extends Plugin implements MapperPlugin {
        public NonDynamicFieldPlugin() {}

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Map.of(NonDynamicFieldMapper.NAME, NonDynamicFieldMapper.PARSER);
        }
    }

    private static class NonDynamicFieldMapper extends FieldMapper {
        private static final String NAME = "non_dynamic";

        private static final TypeParser PARSER = new TypeParser(
            (n, c) -> new Builder(n),
            List.of(notFromDynamicTemplates(NAME), notInMultiFields(NAME))
        );

        private static class Builder extends FieldMapper.Builder {
            private final Parameter<Map<String, String>> meta = Parameter.metaParam();

            Builder(String name) {
                super(name);
            }

            @Override
            protected Parameter<?>[] getParameters() {
                return new Parameter<?>[] { meta };
            }

            @Override
            public NonDynamicFieldMapper build(MapperBuilderContext context) {
                return new NonDynamicFieldMapper(leafName(), new TextFieldMapper.TextFieldType(leafName(), false, true, meta.getValue()));
            }
        }

        private NonDynamicFieldMapper(String simpleName, MappedFieldType mappedFieldType) {
            super(simpleName, mappedFieldType, BuilderParams.empty());
        }

        @Override
        protected String contentType() {
            return NAME;
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) throws IOException {}

        @Override
        public FieldMapper.Builder getMergeBuilder() {
            return new Builder(leafName()).init(this);
        }
    }
}
