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

import com.google.common.collect.UnmodifiableIterator;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.fieldNames;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 * A mapper that indexes the field names of a document under <code>_field_names</code>. This mapper is typically useful in order
 * to have fast <code>exists</code> and <code>missing</code> queries/filters.
 *
 * Added in Elasticsearch 1.3.
 */
public class FieldNamesFieldMapper extends AbstractFieldMapper<String> implements InternalMapper, RootMapper {

    public static final String NAME = "_field_names";

    public static final String CONTENT_TYPE = "_field_names";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = FieldNamesFieldMapper.NAME;
        public static final String INDEX_NAME = FieldNamesFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);
        public static final FieldType FIELD_TYPE_PRE_1_3_0;

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            FIELD_TYPE.freeze();
            FIELD_TYPE_PRE_1_3_0 = new FieldType(FIELD_TYPE);
            FIELD_TYPE_PRE_1_3_0.setIndexed(false);
            FIELD_TYPE_PRE_1_3_0.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, FieldNamesFieldMapper> {

        private boolean indexIsExplicit;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE));
            indexName = Defaults.INDEX_NAME;
        }

        @Override
        public Builder index(boolean index) {
            indexIsExplicit = true;
            return super.index(index);
        }

        @Override
        public FieldNamesFieldMapper build(BuilderContext context) {
            if ((context.indexCreatedVersion() == null || context.indexCreatedVersion().before(Version.V_1_3_0)) && !indexIsExplicit) {
                fieldType.setIndexed(false);
            }
            return new FieldNamesFieldMapper(name, indexName, boost, fieldType, postingsProvider, docValuesProvider, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_1_3_0)) {
                FieldNamesFieldMapper.Builder builder = fieldNames();
                parseField(builder, builder.name, node, parserContext);
                return builder;
            } else {
              throw new ElasticsearchIllegalArgumentException("type="+CONTENT_TYPE+" is not supported on indices created before version 1.3.0 is your cluster running multiple datanode versions?");
            }
        }
    }

    private final FieldType defaultFieldType;

    private static FieldType defaultFieldType(Settings indexSettings) {
        return indexSettings != null && Version.indexCreated(indexSettings).onOrAfter(Version.V_1_3_0) ? Defaults.FIELD_TYPE : Defaults.FIELD_TYPE_PRE_1_3_0;
    }

    public FieldNamesFieldMapper(Settings indexSettings) {
        this(Defaults.NAME, Defaults.INDEX_NAME, indexSettings);
    }

    protected FieldNamesFieldMapper(String name, String indexName, Settings indexSettings) {
        this(name, indexName, Defaults.BOOST, new FieldType(defaultFieldType(indexSettings)), null, null, null, indexSettings);
    }

    public FieldNamesFieldMapper(String name, String indexName, float boost, FieldType fieldType, PostingsFormatProvider postingsProvider,
                           DocValuesFormatProvider docValuesProvider, @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(name, indexName, indexName, name), boost, fieldType, null, Lucene.KEYWORD_ANALYZER,
                Lucene.KEYWORD_ANALYZER, postingsProvider, docValuesProvider, null, null, fieldDataSettings, indexSettings);
        this.defaultFieldType = defaultFieldType(indexSettings);
    }

    @Override
    public FieldType defaultFieldType() {
        return defaultFieldType;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("string");
    }

    @Override
    public String value(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public boolean useTermQueryWithQueryString() {
        return true;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // we parse in post parse
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    static Iterable<String> extractFieldNames(final String fullPath) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new UnmodifiableIterator<String>() {

                    int endIndex = nextEndIndex(0);

                    private int nextEndIndex(int index) {
                        while (index < fullPath.length() && fullPath.charAt(index) != '.') {
                            index += 1;
                        }
                        return index;
                    }

                    @Override
                    public boolean hasNext() {
                        return endIndex <= fullPath.length();
                    }

                    @Override
                    public String next() {
                        final String result = fullPath.substring(0, endIndex);
                        endIndex = nextEndIndex(endIndex + 1);
                        return result;
                    }

                };
            }
        };
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!fieldType.indexed() && !fieldType.stored() && !hasDocValues()) {
            return;
        }
        for (ParseContext.Document document : context.docs()) {
            final List<String> paths = new ArrayList<>();
            for (IndexableField field : document.getFields()) {
                paths.add(field.name());
            }
            for (String path : paths) {
                for (String fieldName : extractFieldNames(path)) {
                    if (fieldType.indexed() || fieldType.stored()) {
                        document.add(new Field(names().indexName(), fieldName, fieldType));
                    }
                    if (hasDocValues()) {
                        document.add(new SortedSetDocValuesField(names().indexName(), new BytesRef(fieldName)));
                    }
                }
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder json = XContentFactory.jsonBuilder();
        super.toXContent(json, params);
        if (json.string().equals("\"" + NAME + "\"{\"type\":\"" + CONTENT_TYPE + "\"}")) {
            return builder;
        }
        return super.toXContent(builder, params);
    }

    @Override
    public boolean isGenerated() {
        return true;
    }
}
