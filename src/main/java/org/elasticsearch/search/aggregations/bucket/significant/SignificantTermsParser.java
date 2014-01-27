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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.support.numeric.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 *
 */
public class SignificantTermsParser implements Aggregator.Parser {

    @Override
    public String type() {
        return SignificantStringTerms.TYPE.name();
    }

    public static final int DEFAULT_REQUIRED_SIZE=10;
    public static final int DEFAULT_SHARD_SIZE=0;
    //Typically need more than one occurrence of something for it to be statistically significant
    public static final int DEFAULT_MIN_DOC_COUNT = 3;
    
    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        int requiredSize = DEFAULT_REQUIRED_SIZE;
        int shardSize = DEFAULT_SHARD_SIZE;
        String format = null;
        String include = null;
        int includeFlags = 0; // 0 means no flags
        String exclude = null;
        int excludeFlags = 0; // 0 means no flags
        String executionHint = null;
        long minDocCount = DEFAULT_MIN_DOC_COUNT; 

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("format".equals(currentFieldName)) {
                    format = parser.text();
                } else if ("include".equals(currentFieldName)) {
                    include = parser.text();
                } else if ("exclude".equals(currentFieldName)) {
                    exclude = parser.text();
                } else if ("execution_hint".equals(currentFieldName) || "executionHint".equals(currentFieldName)) {
                    executionHint = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                } else if ("shard_size".equals(currentFieldName) || "shardSize".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                } else if ("min_doc_count".equals(currentFieldName) || "minDocCount".equals(currentFieldName)) {
                    minDocCount = parser.intValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                  if ("include".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("pattern".equals(currentFieldName)) {
                                include = parser.text();
                            } else if ("flags".equals(currentFieldName)) {
                                includeFlags = Regex.flagsFromString(parser.text());
                            }
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("flags".equals(currentFieldName)) {
                                includeFlags = parser.intValue();
                            }
                        }
                    }
                } else if ("exclude".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("pattern".equals(currentFieldName)) {
                                exclude = parser.text();
                            } else if ("flags".equals(currentFieldName)) {
                                excludeFlags = Regex.flagsFromString(parser.text());
                            }
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("flags".equals(currentFieldName)) {
                                excludeFlags = parser.intValue();
                            }
                        }
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (shardSize == DEFAULT_SHARD_SIZE) {
            //The user has not made a shardSize selection .
            //Use default heuristic to avoid any wrong-ranking caused by distributed counting
            //but request double the usual amount.
            //We typically need more than the number of "top" terms requested by other aggregations
            //as the significance algorithm is in less of a position to down-select at shard-level -
            //some of the things we want to find have only one occurrence on each shard and as
            // such are impossible to differentiate from non-significant terms at that early stage.
            shardSize = 2 * BucketUtils.suggestShardSideQueueSize(requiredSize, context.numberOfShards());
            
        }

        // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }

        IncludeExclude includeExclude = null;
        if (include != null || exclude != null) {
            Pattern includePattern =  include != null ? Pattern.compile(include, includeFlags) : null;
            Pattern excludePattern = exclude != null ? Pattern.compile(exclude, excludeFlags) : null;
            includeExclude = new IncludeExclude(includePattern, excludePattern);
        }


        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            ValuesSourceConfig<?> config = new ValuesSourceConfig<BytesValuesSource>(BytesValuesSource.class);
            config.unmapped(true);
            return new SignificantTermsAggregatorFactory(aggregationName, config, requiredSize, shardSize, minDocCount, includeExclude, executionHint);
        }
        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);

        ValuesSourceConfig<?> config;

        if (mapper instanceof DateFieldMapper) {
            DateFieldMapper dateMapper = (DateFieldMapper) mapper;
            ValueFormatter formatter = format == null ?
                    new ValueFormatter.DateTime(dateMapper.dateTimeFormatter()) :
                    new ValueFormatter.DateTime(format);
            config = new ValuesSourceConfig<NumericValuesSource>(NumericValuesSource.class)
                    .formatter(formatter)
                    .parser(new ValueParser.DateMath(dateMapper.dateMathParser()));

        } else if (mapper instanceof IpFieldMapper) {
            config = new ValuesSourceConfig<NumericValuesSource>(NumericValuesSource.class)
                    .formatter(ValueFormatter.IPv4)
                    .parser(ValueParser.IPv4);

        } else if (indexFieldData instanceof IndexNumericFieldData) {
            config = new ValuesSourceConfig<NumericValuesSource>(NumericValuesSource.class);
            if (format != null) {
                config.formatter(new ValueFormatter.Number.Pattern(format));
            }

        } else {
            config = new ValuesSourceConfig<BytesValuesSource>(BytesValuesSource.class);
            // TODO: it will make sense to set false instead here if the aggregator factory uses
            // ordinals instead of hash tables
            config.needsHashes(true);
        }

        config.fieldContext(new FieldContext(field, indexFieldData));
        // We need values to be unique to be able to run terms aggs efficiently
        config.ensureUnique(true);
        
        return new SignificantTermsAggregatorFactory(aggregationName, config, requiredSize, shardSize, minDocCount, includeExclude, executionHint);
    }


}
