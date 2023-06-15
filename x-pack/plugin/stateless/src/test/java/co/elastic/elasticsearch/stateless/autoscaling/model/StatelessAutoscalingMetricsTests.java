/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.model;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class StatelessAutoscalingMetricsTests extends AbstractWireSerializingTestCase<StatelessAutoscalingMetrics> {

    @Override
    protected Writeable.Reader<StatelessAutoscalingMetrics> instanceReader() {
        return StatelessAutoscalingMetrics::new;
    }

    @Override
    protected StatelessAutoscalingMetrics createTestInstance() {
        return new StatelessAutoscalingMetrics(randomMap(1, 10, () -> Tuple.tuple(randomIdentifier(), randomTierMetrics())));
    }

    private static TierMetrics randomTierMetrics() {
        return new TierMetrics(randomMap(1, 10, () -> Tuple.tuple(randomIdentifier(), MetricsTests.randomMetric())));
    }

    @Override
    protected StatelessAutoscalingMetrics mutateInstance(StatelessAutoscalingMetrics instance) {
        assert instance.tiers().isEmpty() == false : "Should not generate empty tiers";
        return switch (randomInt(3)) {
            // add a tier
            case 0 -> new StatelessAutoscalingMetrics(
                Maps.copyMapWithAddedOrReplacedEntry(instance.tiers(), randomIdentifier(), randomTierMetrics())
            );
            // mutate existing tier
            case 1 -> {
                var tier = randomFrom(instance.tiers().entrySet());
                yield new StatelessAutoscalingMetrics(
                    Maps.copyMapWithAddedOrReplacedEntry(instance.tiers(), tier.getKey(), mutateTierMetrics(tier.getValue()))
                );
            }
            // remove a tier
            case 2 -> new StatelessAutoscalingMetrics(
                Maps.copyMapWithRemovedEntry(instance.tiers(), randomFrom(instance.tiers().keySet()))
            );
            default -> randomValueOtherThan(instance, this::createTestInstance);
        };
    }

    private TierMetrics mutateTierMetrics(TierMetrics instance) {
        assert instance.metrics().isEmpty() == false : "Should not generate empty metrics";
        return switch (randomInt(3)) {
            // add a metric
            case 0 -> new TierMetrics(
                Maps.copyMapWithAddedOrReplacedEntry(instance.metrics(), randomIdentifier(), MetricsTests.randomMetric())
            );
            // mutate existing metric
            case 1 -> new TierMetrics(
                Maps.copyMapWithAddedOrReplacedEntry(
                    instance.metrics(),
                    randomFrom(instance.metrics().keySet()),
                    MetricsTests.randomMetric()
                )
            );
            // remove a metric
            case 2 -> new TierMetrics(Maps.copyMapWithRemovedEntry(instance.metrics(), randomFrom(instance.metrics().keySet())));
            default -> randomValueOtherThan(instance, StatelessAutoscalingMetricsTests::randomTierMetrics);
        };
    }
}
