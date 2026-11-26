/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.GroupedLimitOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.MMROperator;
import org.elasticsearch.compute.operator.MetricsInfoOperator;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ParallelHashAggregationOperator;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.compute.operator.SampleOperator;
import org.elasticsearch.compute.operator.StreamingPageOperator;
import org.elasticsearch.compute.operator.TsInfoOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.compute.operator.fuse.LinearScoreEvalOperator;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupQueryOperator;
import org.elasticsearch.compute.operator.topn.GroupedTopNOperatorStatus;
import org.elasticsearch.compute.operator.topn.TopNOperatorStatus;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.ExternalFieldExtractOperator;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupOperator;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexOperator;
import org.elasticsearch.xpack.esql.enrich.StreamingLookupFromIndexOperator;

import java.util.List;

/**
 * The schema-aware counterpart of ES|QL's named-writeable operator status registry.
 */
public final class EsqlProfileSchemaRegistry {

    private EsqlProfileSchemaRegistry() {}

    /** All operator status variants registered by ES|QL. */
    public static List<ProfileSchema.Variant<Operator.Status>> operatorStatusVariants() {
        return List.of(
            variant(AbstractPageMappingOperator.Status.ENTRY, AbstractPageMappingOperator.Status.PROFILE_SCHEMA),
            variant(FetchDocsSourceOperator.Status.ENTRY, FetchDocsSourceOperator.Status.PROFILE_SCHEMA),
            variant(AbstractPageMappingToIteratorOperator.Status.ENTRY, AbstractPageMappingToIteratorOperator.Status.PROFILE_SCHEMA),
            variant(AggregationOperator.Status.ENTRY, AggregationOperator.Status.PROFILE_SCHEMA),
            variant(ExchangeSinkOperator.Status.ENTRY, ExchangeSinkOperator.Status.PROFILE_SCHEMA),
            variant(ExchangeSourceOperator.Status.ENTRY, ExchangeSourceOperator.Status.PROFILE_SCHEMA),
            variant(StreamingPageOperator.Status.ENTRY, StreamingPageOperator.Status.PROFILE_SCHEMA),
            variant(AsyncExternalSourceOperator.Status.ENTRY, AsyncExternalSourceOperator.Status.PROFILE_SCHEMA),
            variant(ExternalFieldExtractOperator.Status.ENTRY, ExternalFieldExtractOperator.Status.PROFILE_SCHEMA),
            variant(HashAggregationOperator.Status.ENTRY, HashAggregationOperator.Status.PROFILE_SCHEMA),
            variant(LimitOperator.Status.ENTRY, LimitOperator.Status.PROFILE_SCHEMA),
            variant(GroupedLimitOperator.Status.ENTRY, GroupedLimitOperator.Status.PROFILE_SCHEMA),
            variant(GroupedTopNOperatorStatus.ENTRY, GroupedTopNOperatorStatus.PROFILE_SCHEMA),
            variant(LuceneOperator.Status.ENTRY, LuceneOperator.Status.PROFILE_SCHEMA),
            variant(TopNOperatorStatus.ENTRY, TopNOperatorStatus.PROFILE_SCHEMA),
            variant(MvExpandOperator.Status.ENTRY, MvExpandOperator.Status.PROFILE_SCHEMA),
            variant(ValuesSourceReaderOperatorStatus.ENTRY, ValuesSourceReaderOperatorStatus.PROFILE_SCHEMA),
            variant(AsyncOperator.Status.ENTRY, AsyncOperator.Status.PROFILE_SCHEMA),
            variant(EnrichQuerySourceOperator.Status.ENTRY, EnrichQuerySourceOperator.Status.PROFILE_SCHEMA),
            variant(LookupQueryOperator.Status.ENTRY, LookupQueryOperator.Status.PROFILE_SCHEMA),
            variant(EnrichLookupOperator.Status.ENTRY, EnrichLookupOperator.Status.PROFILE_SCHEMA),
            variant(LookupFromIndexOperator.Status.ENTRY, LookupFromIndexOperator.Status.PROFILE_SCHEMA),
            variant(
                StreamingLookupFromIndexOperator.StreamingLookupStatus.ENTRY,
                StreamingLookupFromIndexOperator.StreamingLookupStatus.PROFILE_SCHEMA
            ),
            variant(SampleOperator.Status.ENTRY, SampleOperator.Status.PROFILE_SCHEMA),
            variant(MetricsInfoOperator.Status.ENTRY, MetricsInfoOperator.Status.PROFILE_SCHEMA),
            variant(TsInfoOperator.Status.ENTRY, TsInfoOperator.Status.PROFILE_SCHEMA),
            variant(LinearScoreEvalOperator.Status.ENTRY, LinearScoreEvalOperator.Status.PROFILE_SCHEMA),
            variant(MMROperator.Status.ENTRY, MMROperator.Status.PROFILE_SCHEMA)
        );
    }

    /** All inline extra-status variants registered by ES|QL. */
    public static List<ProfileSchema.Variant<Operator.Status.ExtraStatus>> extraStatusVariants() {
        return List.of(
            ProfileSchema.Variant.fromEntry(
                Operator.Status.ExtraStatus.class,
                ParallelHashAggregationOperator.PartitioningStatus.ENTRY,
                ParallelHashAggregationOperator.PartitioningStatus.PROFILE_SCHEMA
            )
        );
    }

    private static ProfileSchema.Variant<Operator.Status> variant(
        NamedWriteableRegistry.Entry entry,
        ProfileSchema<? extends Operator.Status> schema
    ) {
        return ProfileSchema.Variant.fromEntry(Operator.Status.class, entry, schema);
    }
}
