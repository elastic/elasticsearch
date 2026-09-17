/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;

import java.time.Instant;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class TransportValidateTransformActionTests extends ESTestCase {

    private static final String TIME_FIELD = "@timestamp";

    private static SourceConfig matchAllSource() {
        return new SourceConfig(
            new String[] { "source-index" },
            new QueryConfig(Map.of(MatchAllQueryBuilder.NAME, Map.of()), new MatchAllQueryBuilder()),
            Map.of(),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            null
        );
    }

    private static TransformConfig configWithSync(SourceConfig source, TimeSyncConfig syncConfig) {
        return new TransformConfig.Builder(TransformConfigTests.randomTransformConfig()).setSource(source)
            .setSyncConfig(syncConfig)
            .build();
    }

    public void testBoundValidationSourceIsUnchangedWithoutFrom() {
        SourceConfig source = matchAllSource();
        TransformConfig config = configWithSync(source, new TimeSyncConfig(TIME_FIELD, TimeValue.timeValueSeconds(60)));

        // No from bound: validation runs over the unmodified source, so we hand back the same instance.
        assertThat(TransportValidateTransformAction.boundValidationSource(config, null), sameInstance(config.getSource()));
    }

    public void testBoundValidationSourceAddsRangeFilterForContinuousTransform() {
        SourceConfig source = matchAllSource();
        TransformConfig config = configWithSync(source, new TimeSyncConfig(TIME_FIELD, TimeValue.timeValueSeconds(60)));
        Instant from = Instant.ofEpochMilli(1_700_000_000_000L);

        SourceConfig bounded = TransportValidateTransformAction.boundValidationSource(config, from);

        QueryBuilder expected = new BoolQueryBuilder().filter(new MatchAllQueryBuilder())
            .filter(new RangeQueryBuilder(TIME_FIELD).gte(from.toEpochMilli()).format("epoch_millis"));
        assertThat(bounded.getQueryConfig().getQuery(), equalTo(expected));
        // Indices and options are preserved so only the time range is narrowed.
        assertThat(bounded.getIndex(), equalTo(config.getSource().getIndex()));
    }

    public void testBoundValidationSourceIsUnchangedForBatchTransform() {
        SourceConfig source = matchAllSource();
        // A batch transform has no sync config; from cannot be honored, so the source is returned as-is.
        TransformConfig config = configWithSync(source, null);

        assertThat(
            TransportValidateTransformAction.boundValidationSource(config, Instant.ofEpochMilli(1_700_000_000_000L)),
            sameInstance(config.getSource())
        );
    }
}
