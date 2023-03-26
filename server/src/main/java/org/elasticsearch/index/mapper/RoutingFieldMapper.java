/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;
import java.util.Map;

public class RoutingFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_routing";
    public static final String CONTENT_TYPE = "_routing";

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public static class Defaults {
        public static final boolean REQUIRED = false;
    }

    private static RoutingFieldMapper toType(FieldMapper in) {
        return (RoutingFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        final Parameter<Boolean> required = Parameter.boolParam("required", false, m -> toType(m).required, Defaults.REQUIRED);

        protected Builder() {
            super(NAME);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { required };
        }

        @Override
        public RoutingFieldMapper build() {
            return RoutingFieldMapper.get(required.getValue());
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> RoutingFieldMapper.get(Defaults.REQUIRED), c -> new Builder());

    public static final MappedFieldType FIELD_TYPE = new RoutingFieldType();

    static final class RoutingFieldType extends StringFieldType {

        private RoutingFieldType() {
            super(NAME, true, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }
    }

    /**
     * Should we require {@code routing} on CRUD operations?
     */
    private final boolean required;

    private static final RoutingFieldMapper REQUIRED = new RoutingFieldMapper(true);
    private static final RoutingFieldMapper NOT_REQUIRED = new RoutingFieldMapper(false);

    private static final Map<String, NamedAnalyzer> ANALYZERS = Map.of(NAME, Lucene.KEYWORD_ANALYZER);

    public static RoutingFieldMapper get(boolean required) {
        return required ? REQUIRED : NOT_REQUIRED;
    }

    private RoutingFieldMapper(boolean required) {
        super(FIELD_TYPE);
        this.required = required;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return ANALYZERS;
    }

    /**
     * Should we require {@code routing} on CRUD operations?
     */
    public boolean required() {
        return this.required;
    }

    @Override
    public void preParse(DocumentParserContext context) {
        String routing = context.sourceToParse().routing();
        if (routing != null) {
            context.doc().add(new StringField(fieldType().name(), routing, Field.Store.YES));
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }
}
