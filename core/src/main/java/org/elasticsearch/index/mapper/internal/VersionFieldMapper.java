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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Mapper for the _version field. */
public class VersionFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_version";
    public static final String CONTENT_TYPE = "_version";

    public static class Defaults {

        public static final String NAME = VersionFieldMapper.NAME;
        public static final MappedFieldType FIELD_TYPE = new VersionFieldType();

        static {
            FIELD_TYPE.setNames(new MappedFieldType.Names(NAME));
            FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, VersionFieldMapper> {

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
        }

        @Override
        public VersionFieldMapper build(BuilderContext context) {
            return new VersionFieldMapper(context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder();
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                if (fieldName.equals("doc_values_format") && parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
                    // ignore in 1.x, reject in 2.x
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    static final class VersionFieldType extends MappedFieldType {

        public VersionFieldType() {
            setFieldDataType(new FieldDataType("long"));
        }

        protected VersionFieldType(VersionFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new VersionFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Long value(Object value) {
            if (value == null || (value instanceof Long)) {
                return (Long) value;
            } else {
                return Long.parseLong(value.toString());
            }
        }
    }

    public VersionFieldMapper(Settings indexSettings) {
        super(NAME, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE, indexSettings);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        // see InternalEngine.updateVersion to see where the real version value is set
        final Field version = new NumericDocValuesField(NAME, -1L);
        context.version(version);
        fields.add(version);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // _version added in preparse
        return null;
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // In the case of nested docs, let's fill nested docs with version=1 so that Lucene doesn't write a Bitset for documents
        // that don't have the field. This is consistent with the default value for efficiency.
        for (int i = 1; i < context.docs().size(); i++) {
            final Document doc = context.docs().get(i);
            doc.add(new NumericDocValuesField(NAME, 1L));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        // nothing to do
    }
}
