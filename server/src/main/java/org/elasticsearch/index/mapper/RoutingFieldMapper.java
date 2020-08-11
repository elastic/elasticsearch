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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RoutingFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_routing";
    public static final String CONTENT_TYPE = "_routing";

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
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
        public RoutingFieldMapper build(BuilderContext context) {
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
            super(NAME, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }
    }

    private final boolean required;

    private RoutingFieldMapper(boolean required) {
        super(RoutingFieldType.INSTANCE);
        this.required = required;
    }

    public boolean required() {
        return this.required;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // no need ot parse here, we either get the routing in the sourceToParse
        // or we don't have routing, if we get it in sourceToParse, we process it in preParse
        // which will always be called
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String routing = context.sourceToParse().routing();
        if (routing != null) {
            context.doc().add(new Field(fieldType().name(), routing, Defaults.FIELD_TYPE));
            createFieldNamesField(context);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
