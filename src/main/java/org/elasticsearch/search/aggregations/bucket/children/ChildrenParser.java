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

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class ChildrenParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalChildren.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
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
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (childType == null) {
            throw new SearchParseException(context, "Missing [child_type] field for children aggregation [" + aggregationName + "]");
        }

        ValuesSourceConfig<ValuesSource.Bytes.WithOrdinals.ParentChild> config = new ValuesSourceConfig<>(ValuesSource.Bytes.WithOrdinals.ParentChild.class);
        DocumentMapper childDocMapper = context.mapperService().documentMapper(childType);

        String parentType = null;
        Filter parentFilter = null;
        Filter childFilter = null;
        if (childDocMapper != null) {
            ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
            if (!parentFieldMapper.active()) {
                throw new SearchParseException(context, "[children] _parent field not configured");
            }
            parentType = parentFieldMapper.type();
            DocumentMapper parentDocMapper = context.mapperService().documentMapper(parentType);
            if (parentDocMapper != null) {
                parentFilter = context.filterCache().cache(parentDocMapper.typeFilter(), null, context.queryParserService().autoFilterCachePolicy());
                childFilter = context.filterCache().cache(childDocMapper.typeFilter(), null, context.queryParserService().autoFilterCachePolicy());
                ParentChildIndexFieldData parentChildIndexFieldData = context.fieldData().getForField(parentFieldMapper);
                config.fieldContext(new FieldContext(parentFieldMapper.names().indexName(), parentChildIndexFieldData, parentFieldMapper));
            } else {
                config.unmapped(true);
            }
        } else {
            config.unmapped(true);
        }
        return new ParentToChildrenAggregator.Factory(aggregationName, config, parentType, parentFilter, childFilter);
    }
}
