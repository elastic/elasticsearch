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

package org.elasticsearch.search.aggregations.bucket.children;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.ParentChild;

import java.io.IOException;
import java.util.Objects;

public class ChildrenAggregatorBuilder extends ValuesSourceAggregatorBuilder<ParentChild, ChildrenAggregatorBuilder> {

    static final ChildrenAggregatorBuilder PROTOTYPE = new ChildrenAggregatorBuilder("", "");

    private String parentType;
    private final String childType;
    private Query parentFilter;
    private Query childFilter;

    /**
     * @param name
     *            the name of this aggregation
     * @param childType
     *            the type of children documents
     */
    public ChildrenAggregatorBuilder(String name, String childType) {
        super(name, InternalChildren.TYPE, ValuesSourceType.BYTES, ValueType.STRING);
        if (childType == null) {
            throw new IllegalArgumentException("[childType] must not be null: [" + name + "]");
        }
        this.childType = childType;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ParentChild, ?> innerBuild(AggregationContext context,
            ValuesSourceConfig<ParentChild> config, AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new ChildrenAggregatorFactory(name, type, config, parentType, childFilter, parentFilter, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected ValuesSourceConfig<ParentChild> resolveConfig(AggregationContext aggregationContext) {
        ValuesSourceConfig<ParentChild> config = new ValuesSourceConfig<>(ValuesSourceType.BYTES);
        DocumentMapper childDocMapper = aggregationContext.searchContext().mapperService().documentMapper(childType);

        if (childDocMapper != null) {
            ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
            if (!parentFieldMapper.active()) {
                throw new IllegalArgumentException("[children] no [_parent] field not configured that points to a parent type");
            }
            parentType = parentFieldMapper.type();
            DocumentMapper parentDocMapper = aggregationContext.searchContext().mapperService().documentMapper(parentType);
            if (parentDocMapper != null) {
                parentFilter = parentDocMapper.typeFilter();
                childFilter = childDocMapper.typeFilter();
                ParentChildIndexFieldData parentChildIndexFieldData = aggregationContext.searchContext().fieldData()
                        .getForField(parentFieldMapper.fieldType());
                config.fieldContext(new FieldContext(parentFieldMapper.fieldType().name(), parentChildIndexFieldData,
                        parentFieldMapper.fieldType()));
            } else {
                config.unmapped(true);
            }
        } else {
            config.unmapped(true);
        }
        return config;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ParentToChildrenAggregator.TYPE_FIELD.getPreferredName(), childType);
        return builder;
    }

    @Override
    protected ChildrenAggregatorBuilder innerReadFrom(String name, ValuesSourceType valuesSourceType,
            ValueType targetValueType, StreamInput in) throws IOException {
        String childType = in.readString();
        ChildrenAggregatorBuilder factory = new ChildrenAggregatorBuilder(name, childType);
        return factory;
    }

    @Override
    protected void writeEnd2(StreamOutput out) throws IOException {
        out.writeString(childType);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(childType);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        ChildrenAggregatorBuilder other = (ChildrenAggregatorBuilder) obj;
        return Objects.equals(childType, other.childType);
    }

}