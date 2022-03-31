/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.ArrayList;
import java.util.List;

public class RankEvalNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.add(
            new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(PrecisionAtK.NAME), PrecisionAtK::fromXContent)
        );
        namedXContent.add(new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(RecallAtK.NAME), RecallAtK::fromXContent));
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(MeanReciprocalRank.NAME),
                MeanReciprocalRank::fromXContent
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(DiscountedCumulativeGain.NAME),
                DiscountedCumulativeGain::fromXContent
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(ExpectedReciprocalRank.NAME),
                ExpectedReciprocalRank::fromXContent
            )
        );

        namedXContent.add(
            new NamedXContentRegistry.Entry(MetricDetail.class, new ParseField(PrecisionAtK.NAME), PrecisionAtK.Detail::fromXContent)
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(MetricDetail.class, new ParseField(RecallAtK.NAME), RecallAtK.Detail::fromXContent)
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                MetricDetail.class,
                new ParseField(MeanReciprocalRank.NAME),
                MeanReciprocalRank.Detail::fromXContent
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                MetricDetail.class,
                new ParseField(DiscountedCumulativeGain.NAME),
                DiscountedCumulativeGain.Detail::fromXContent
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                MetricDetail.class,
                new ParseField(ExpectedReciprocalRank.NAME),
                ExpectedReciprocalRank.Detail::fromXContent
            )
        );
        return namedXContent;
    }
}
