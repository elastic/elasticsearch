/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rollup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RollupGroup;
import org.elasticsearch.cluster.metadata.RollupMetadata;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.SortedMap;

public class RollupShardDecider {
    private static final Logger logger = LogManager.getLogger(RollupShardDecider.class);

    public static boolean shouldMatchRollup(QueryShardContext context,
                                            QueryBuilder queryBuilder,
                                            AggregatorFactories.Builder aggFactoryBuilders,
                                            RollupMetadata rollupMetadata,
                                            Map<String, String> indexRollupMetadata,
                                            IndexMetadata requestIndexMetadata,
                                            String[] indices,
                                            SortedMap<String, IndexAbstraction> indexLookup) {
        // TODO(talevy): currently assumes that all indices part of the same rollup group are searched as well, since this is the case
        //  for data-streams. should not be here in the non-datastream case...

        if (aggFactoryBuilders == null && indexRollupMetadata != null) {
            return false;
        } else if (aggFactoryBuilders != null && rollupMetadata != null && indexRollupMetadata != null && queryBuilder != null) {
            Query query = context.toQuery(queryBuilder).query();
            try {
                AggregatorFactory[] factories = aggFactoryBuilders
                    .build(new AggregationContext.ProductionAggregationContext(context, query), null)
                    .factories();
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to create aggregators, shard not supported", e);
            }

            if (queryShard(rollupMetadata, indices, requestIndexMetadata, aggFactoryBuilders) == false) {
                return false;
            }
            // do something with query?
            query.visit(new QueryVisitor() {
                @Override
                public void consumeTerms(Query query, Term... terms) {
                    super.consumeTerms(query, terms);
                }
            });
        } else if (aggFactoryBuilders != null && rollupMetadata != null) {
            if (queryShard(rollupMetadata, indices, requestIndexMetadata, aggFactoryBuilders) == false) {
                return false;
            }
        }

        return true;
    }

    static boolean queryShard(RollupMetadata rollupMetadata, String[] indices, IndexMetadata requestIndexMetadata,
                              AggregatorFactories.Builder aggFactoryBuilders) {
        String requestIndexName = requestIndexMetadata.getIndex().getName();
        Map<String, String> indexRollupMetadata = requestIndexMetadata.getCustomData(RollupMetadata.TYPE);
        boolean isRollupIndex = requestIndexMetadata.getCustomData(RollupMetadata.TYPE) != null;
        final String originalIndexName;
        final RollupGroup rollupGroup;
        if (isRollupIndex) {
            // rollup is being searched
            originalIndexName = indexRollupMetadata.get(RollupMetadata.SOURCE_INDEX_NAME_META_FIELD);
            rollupGroup = rollupMetadata.rollupGroups().get(originalIndexName);
        } else if (rollupMetadata.contains(requestIndexName)) {
            originalIndexName = requestIndexName;
            rollupGroup = rollupMetadata.rollupGroups().get(requestIndexName);
        } else {
            // not part of a rollup group, search away!
            return true;
        }
        String bestIndexForAgg = bestIndexForAgg(originalIndexName, rollupGroup, aggFactoryBuilders);
        logger.warn("original index: " + originalIndexName);
        logger.warn("best index: " + bestIndexForAgg);
        return requestIndexName.equals(bestIndexForAgg);
    }

    static String bestIndexForAgg(String originalIndexName, RollupGroup rollupGroup, AggregatorFactories.Builder aggFactoryBuilders) {
        for (AggregationBuilder builder : aggFactoryBuilders.getAggregatorFactories()) {
            if (builder.getWriteableName().equals(DateHistogramAggregationBuilder.NAME)) {
                return bestIndexForDateHisto(originalIndexName, rollupGroup, (DateHistogramAggregationBuilder) builder);
            }
        }
        return originalIndexName;
    }

    static String bestIndexForDateHisto(String originalIndexName, RollupGroup rollupGroup, DateHistogramAggregationBuilder source) {
        DateHistogramInterval maxInterval = null;
        String bestIndex = originalIndexName;
        for (String rollupIndex : rollupGroup.getIndices()) {
            DateHistogramInterval thisInterval = new DateHistogramInterval(rollupGroup.getDateInterval(rollupIndex));
            ZoneId thisTimezone = ZoneId.of(rollupGroup.getDateTimezone(rollupIndex));

            ZoneId sourceTimeZone = source.timeZone() == null ? ZoneOffset.UTC : ZoneId.of(source.timeZone().toString(), ZoneId.SHORT_IDS);
            DateHistogramInterval sourceInterval = source.getCalendarInterval();

            if (thisTimezone.getRules().equals(sourceTimeZone.getRules()) == false) {
                continue;
            }
            if (validateCalendarInterval(sourceInterval, thisInterval)) {
                if (maxInterval == null) {
                    bestIndex = rollupIndex;
                    maxInterval = thisInterval;
                } else if (validateCalendarInterval(thisInterval, maxInterval)) {
                    bestIndex = rollupIndex;
                    maxInterval = thisInterval;
                }
            }
        }
        return bestIndex;
    }

    static boolean validateCalendarInterval(DateHistogramInterval requestInterval,
                                            DateHistogramInterval configInterval) {
        if (requestInterval == null || configInterval == null) {
            return false;
        }

        // The request must be gte the config.  The CALENDAR_ORDERING map values are integers representing
        // relative orders between the calendar units
        Rounding.DateTimeUnit requestUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(requestInterval.toString());
        if (requestUnit == null) {
            return false;
        }
        Rounding.DateTimeUnit configUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(configInterval.toString());
        if (configUnit == null) {
            return false;
        }

        long requestOrder = requestUnit.getField().getBaseUnit().getDuration().toMillis();
        long configOrder = configUnit.getField().getBaseUnit().getDuration().toMillis();

        // All calendar units are multiples naturally, so we just care about gte
        return requestOrder >= configOrder;
    }
}
