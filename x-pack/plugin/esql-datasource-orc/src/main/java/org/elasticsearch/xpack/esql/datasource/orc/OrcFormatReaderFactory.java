/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.List;
import java.util.Map;

/**
 * Resource-free ORC factory. Each {@link #create} returns a distinct reader.
 */
public final class OrcFormatReaderFactory implements FormatReaderFactory {

    private static final AggregatePushdownSupport AGGREGATE_PUSHDOWN = new OrcAggregatePushdownSupport();
    private static final FilterPushdownSupport FILTER_PUSHDOWN = new OrcFilterPushdownSupport();

    @Override
    public FormatReader create(Settings settings, BlockFactory blockFactory) {
        return new OrcFormatReader(blockFactory);
    }

    @Override
    public FormatReader create(
        Settings settings,
        BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        @Nullable FormatReadContext.Binding binding
    ) {
        FormatReadContext.Binding bound = binding == null ? FormatReadContext.Binding.empty() : binding;
        SearchArgument sarg = null;
        OrcPushedExpressions expressions = null;
        if (bound.pushedFilter() instanceof SearchArgument searchArgument) {
            sarg = searchArgument;
        } else if (bound.pushedFilter() instanceof OrcPushedExpressions pushed) {
            expressions = new OrcPushedExpressions(List.copyOf(pushed.expressions()));
        }
        return new OrcFormatReader(
            blockFactory,
            sarg,
            expressions,
            bound.dynamicThreshold(),
            bound.declaredDateFormats(),
            bound.declaredTypeColumns(),
            new OrcReaderCounters()
        );
    }

    @Override
    public String formatName() {
        return "orc";
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return AGGREGATE_PUSHDOWN;
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        return FILTER_PUSHDOWN;
    }

    @Override
    public boolean dropsRowsUnderPushedFilter() {
        return true;
    }

    @Override
    public boolean rangeAware() {
        return true;
    }

    @Override
    public boolean acceptsDynamicThreshold() {
        return true;
    }

    @Override
    public boolean supportsWholeFileCompression() {
        return false;
    }
}
