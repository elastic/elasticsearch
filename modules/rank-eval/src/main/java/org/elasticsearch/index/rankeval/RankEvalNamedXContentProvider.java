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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.ArrayList;
import java.util.List;

public class RankEvalNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.add(new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(PrecisionAtK.NAME),
                PrecisionAtK::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(RecallAtK.NAME),
                RecallAtK::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(MeanReciprocalRank.NAME),
                MeanReciprocalRank::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(DiscountedCumulativeGain.NAME),
                DiscountedCumulativeGain::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(ExpectedReciprocalRank.NAME),
                ExpectedReciprocalRank::fromXContent));

        namedXContent.add(new NamedXContentRegistry.Entry(MetricDetail.class, new ParseField(PrecisionAtK.NAME),
                PrecisionAtK.Detail::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(MetricDetail.class, new ParseField(RecallAtK.NAME),
                RecallAtK.Detail::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(MetricDetail.class, new ParseField(MeanReciprocalRank.NAME),
                MeanReciprocalRank.Detail::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(MetricDetail.class, new ParseField(DiscountedCumulativeGain.NAME),
                DiscountedCumulativeGain.Detail::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(MetricDetail.class, new ParseField(ExpectedReciprocalRank.NAME),
                ExpectedReciprocalRank.Detail::fromXContent));
        return namedXContent;
    }
}
