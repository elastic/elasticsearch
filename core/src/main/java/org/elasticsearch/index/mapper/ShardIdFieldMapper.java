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

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.IdFieldMapper.IdFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A mapper that stores the shard id that documents are assigned to.
 * This field is doc-valued instead of being a view like _index so that (shard_id, seq_no) pairs are still unique after shrinking.
 */
public class ShardIdFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_shard";

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node,
                ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            assert fieldType == null || fieldType.equals(new ShardIdFieldType());
            final IndexSettings indexSettings = context.mapperService().getIndexSettings();
            return new ShardIdFieldMapper(indexSettings);
        }
    }

    static class ShardIdFieldType extends MappedFieldType {

        ShardIdFieldType() {
            setName(NAME);
            setHasDocValues(true);
            setIndexOptions(IndexOptions.NONE);
        }

        protected ShardIdFieldType(ShardIdFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new ShardIdFieldType(this);
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            BytesRef binaryValue;
            if (value instanceof BytesRef) {
                binaryValue = (BytesRef) value;
            } else {
                binaryValue = new BytesRef(value.toString());
            }

            if (context.indexVersionCreated().before(Version.V_6_0_0_beta1)) {
                if (binaryValue.bytesEquals(new BytesRef(Integer.toString(context.getShardId())))) {
                    return new MatchAllDocsQuery();
                } else {
                    return new MatchNoDocsQuery("Different shard than the requested one");
                }
            } else {
                // We use doc values for querying.
                // This is fine since in the common case either we are on the right shard
                // and we will return the doc values iterator, or we are on a different shard
                // and we will return a null scorer since the term can't be found in the terms
                // dict.
                return SortedDocValuesField.newExactQuery(NAME, binaryValue);
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            IndexFieldData.Builder view = new ConstantIndexFieldData.Builder((mapperService, shardId) -> Integer.toString(shardId));
            IndexFieldData.Builder dv = new DocValuesIndexFieldData.Builder();
            return new IndexFieldData.Builder() {
                @Override
                public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                        CircuitBreakerService breakerService, MapperService mapperService, int shardId) {
                    if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_6_0_0_beta1)) {
                        return dv.build(indexSettings, fieldType, cache, breakerService, mapperService, shardId);
                    } else {
                        if (shardId == -1) {
                            throw new IllegalArgumentException("The shard id is not usable with 5.x indices and index sorting or scripts");
                        }
                        return view.build(indexSettings, fieldType, cache, breakerService, mapperService, shardId);
                    }
                }
            };
        }
    }

    private final boolean pre6x;

    protected ShardIdFieldMapper(IndexSettings indexSettings) {
        super(NAME, new ShardIdFieldType(), new ShardIdFieldType(), indexSettings.getSettings());
        pre6x = indexSettings.getIndexVersionCreated().before(Version.V_6_0_0_beta1);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {}

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (pre6x == false) {
            fields.add(new SortedDocValuesField(NAME, new BytesRef(Integer.toString(context.sourceToParse().shardId()))));
        }
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // nothing to serialize since it is not configurable
        return builder;
    }
}
