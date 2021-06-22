/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;
import java.util.List;

public class RoutingFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_routing";
    public static final String CONTENT_TYPE = "_routing";

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public static class Defaults {

        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

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
        protected List<Parameter<?>> getParameters() {
            return List.of(required);
        }

        @Override
        public RoutingFieldMapper build() {
            return new RoutingFieldMapper(required.getValue());
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> new RoutingFieldMapper(Defaults.REQUIRED),
        c -> new Builder()
    );

    static final class RoutingFieldType extends StringFieldType {

        static RoutingFieldType INSTANCE = new RoutingFieldType();

        private RoutingFieldType() {
            super(NAME, true, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }
    }

    private final boolean required;

    private RoutingFieldMapper(boolean required) {
        super(RoutingFieldType.INSTANCE, Lucene.KEYWORD_ANALYZER);
        this.required = required;
    }

    public boolean required() {
        return this.required;
    }

    @Override
    public void preParse(DocumentParserContext context) {
        String routing = context.sourceToParse().routing();
        if (context.indexSettings().inTimeSeriesMode()) {
            // TODO when we stop storing the tsid in the routing fail any request with routing in time series mode
            // the routing will always come from the time series id.
            return;
        }
        if (routing != null) {
            context.doc().add(new Field(fieldType().name(), routing, Defaults.FIELD_TYPE));
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
