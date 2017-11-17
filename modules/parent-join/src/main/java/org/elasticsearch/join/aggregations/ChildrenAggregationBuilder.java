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
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.join.mapper.ParentIdFieldMapper;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

public class ChildrenAggregationBuilder
        extends ValuesSourceAggregationBuilder<WithOrdinals, ChildrenAggregationBuilder> {

    public static final String NAME = "children";

    private final String childType;
    private Query parentFilter;
    private Query childFilter;

    /**
     * @param name
     *            the name of this aggregation
     * @param childType
     *            the type of children documents
     */
    public ChildrenAggregationBuilder(String name, String childType) {
        super(name, ValuesSourceType.BYTES, ValueType.STRING);
        if (childType == null) {
            throw new IllegalArgumentException("[childType] must not be null: [" + name + "]");
        }
        this.childType = childType;
    }

    /**
     * Read from a stream.
     */
    public ChildrenAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.BYTES, ValueType.STRING);
        childType = in.readString();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeString(childType);
    }

    @Override
    protected ValuesSourceAggregatorFactory<WithOrdinals, ?> innerBuild(SearchContext context,
                                                                        ValuesSourceConfig<WithOrdinals> config,
                                                                        AggregatorFactory<?> parent,
                                                                        Builder subFactoriesBuilder) throws IOException {
        return new ChildrenAggregatorFactory(name, config, childFilter, parentFilter, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected ValuesSourceConfig<WithOrdinals> resolveConfig(SearchContext context) {
        ValuesSourceConfig<WithOrdinals> config = new ValuesSourceConfig<>(ValuesSourceType.BYTES);
        if (context.mapperService().getIndexSettings().isSingleType()) {
            joinFieldResolveConfig(context, config);
        } else {
            parentFieldResolveConfig(context, config);
        }
        return config;
    }

    private void joinFieldResolveConfig(SearchContext context, ValuesSourceConfig<WithOrdinals> config) {
        ParentJoinFieldMapper parentJoinFieldMapper = ParentJoinFieldMapper.getMapper(context.mapperService());
        ParentIdFieldMapper parentIdFieldMapper = parentJoinFieldMapper.getParentIdFieldMapper(childType, false);
        if (parentIdFieldMapper != null) {
            parentFilter = parentIdFieldMapper.getParentFilter();
            childFilter = parentIdFieldMapper.getChildFilter(childType);
            MappedFieldType fieldType = parentIdFieldMapper.fieldType();
            final SortedSetDVOrdinalsIndexFieldData fieldData = context.getForField(fieldType);
            config.fieldContext(new FieldContext(fieldType.name(), fieldData, fieldType));
        } else {
            config.unmapped(true);
        }
    }

    private void parentFieldResolveConfig(SearchContext context, ValuesSourceConfig<WithOrdinals> config) {
        DocumentMapper childDocMapper = context.mapperService().documentMapper(childType);
        if (childDocMapper != null) {
            ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
            if (!parentFieldMapper.active()) {
                throw new IllegalArgumentException("[children] no [_parent] field not configured that points to a parent type");
            }
            String parentType = parentFieldMapper.type();
            DocumentMapper parentDocMapper = context.mapperService().documentMapper(parentType);
            if (parentDocMapper != null) {
                parentFilter = parentDocMapper.typeFilter(context.getQueryShardContext());
                childFilter = childDocMapper.typeFilter(context.getQueryShardContext());
                MappedFieldType parentFieldType = parentDocMapper.parentFieldMapper().getParentJoinFieldType();
                final SortedSetDVOrdinalsIndexFieldData fieldData = context.getForField(parentFieldType);
                config.fieldContext(new FieldContext(parentFieldType.name(), fieldData,
                    parentFieldType));
            } else {
                config.unmapped(true);
            }
        } else {
            config.unmapped(true);
        }
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ParentToChildrenAggregator.TYPE_FIELD.getPreferredName(), childType);
        return builder;
    }

    public static ChildrenAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
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
                    "Missing [child_type] field for children aggregation [" + aggregationName + "]");
        }

        return new ChildrenAggregationBuilder(aggregationName, childType);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(childType);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        ChildrenAggregationBuilder other = (ChildrenAggregationBuilder) obj;
        return Objects.equals(childType, other.childType);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
