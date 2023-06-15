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
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class MetricsTests extends AbstractWireSerializingTestCase<Metric> {

    @Override
    protected Writeable.Reader<Metric> instanceReader() {
        return Metric::readFrom;
    }

    @Override
    protected Metric createTestInstance() {
        return randomMetric();
    }

    @Override
    protected Metric mutateInstance(Metric instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public static Metric randomMetric() {
        return randomBoolean() ? randomSingularMetric() : randomArrayMetric();
    }

    private static Metric.SingularMetric randomSingularMetric() {
        return new Metric.SingularMetric(randomBoolean() ? randomInt() : randomDouble(), randomFrom(MetricQuality.values()));
    }

    private static Metric.ArrayMetric randomArrayMetric() {
        return new Metric.ArrayMetric(randomList(1, 10, MetricsTests::randomSingularMetric));
    }
}
