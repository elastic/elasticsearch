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

package org.elasticsearch.join.aggregations;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.join.mapper.ParentIdFieldMapper;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ParentAggregationBuilder extends ValuesSourceAggregationBuilder<ParentAggregationBuilder> {

    public static final String NAME = "parent";

    private final String childType;
    private Query parentFilter;
    private Query childFilter;

    /**
     * @param name
     *            the name of this aggregation
     * @param childType
     *            the type of children documents
     */
    public ParentAggregationBuilder(String name, String childType) {
        super(name);
        if (childType == null) {
            throw new IllegalArgumentException("[childType] must not be null: [" + name + "]");
        }
        this.childType = childType;
    }

    protected ParentAggregationBuilder(ParentAggregationBuilder clone,
                                         Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.childType = clone.childType;
        this.childFilter = clone.childFilter;
        this.parentFilter = clone.parentFilter;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ParentAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public ParentAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        childType = in.readString();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeString(childType);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       Builder subFactoriesBuilder) throws IOException {
        return new ParentAggregatorFactory(name, config, childFilter, parentFilter, queryShardContext, parent,
                subFactoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceConfig resolveConfig(QueryShardContext queryShardContext) {
        ValuesSourceConfig config;
        ParentJoinFieldMapper parentJoinFieldMapper = ParentJoinFieldMapper.getMapper(queryShardContext.getMapperService());
        ParentIdFieldMapper parentIdFieldMapper = parentJoinFieldMapper.getParentIdFieldMapper(childType, false);
        if (parentIdFieldMapper != null) {
            parentFilter = parentIdFieldMapper.getParentFilter();
            childFilter = parentIdFieldMapper.getChildFilter(childType);
            MappedFieldType fieldType = parentIdFieldMapper.fieldType();
            config = ValuesSourceConfig.resolveFieldOnly(fieldType, queryShardContext);
        } else {
            // unmapped case
            config = ValuesSourceConfig.resolveUnmapped(defaultValueSourceType(), queryShardContext);
        }
        return config;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ChildrenToParentAggregator.TYPE_FIELD.getPreferredName(), childType);
        return builder;
    }

    public static ParentAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        String childType = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("type".equals(currentFieldName)) {
                    childType = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (childType == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "Missing [child_type] field for parent aggregation [" + aggregationName + "]");
        }

        return new ParentAggregationBuilder(aggregationName, childType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), childType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ParentAggregationBuilder other = (ParentAggregationBuilder) obj;
        return Objects.equals(childType, other.childType);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return ValuesSourceRegistry.UNREGISTERED_KEY;
    }
}
