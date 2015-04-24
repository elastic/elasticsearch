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

package org.elasticsearch.index.mapper.externalvalues;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.*;

import java.io.IOException;
import java.util.Map;

public class ExternalRootMapper implements RootMapper {

    static final String CONTENT_TYPE = "_external_root";
    static final String FIELD_NAME = "_is_external";
    static final String FIELD_VALUE = "true";

    @Override
    public String name() {
        return CONTENT_TYPE;
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        return null;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        if (!(mergeWith instanceof ExternalRootMapper)) {
            mergeResult.addConflict("Trying to merge " + mergeWith + " with " + this);
        }
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public void close() {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(CONTENT_TYPE).endObject();
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        context.doc().add(new StringField(FIELD_NAME, FIELD_VALUE, Store.YES));
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    public static class Builder extends Mapper.Builder<Builder, ExternalRootMapper> {

        protected Builder() {
            super(CONTENT_TYPE);
        }

        @Override
        public ExternalRootMapper build(BuilderContext context) {
            return new ExternalRootMapper();
        }
        
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder();
        }
        
    }

}
