/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.logging;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.logging.LoggingFeatureSetUsage.EsqlLoggingConfig;
import org.elasticsearch.xpack.core.logging.LoggingFeatureSetUsage.LoggingConfig;
import org.elasticsearch.xpack.core.logging.LoggingFeatureSetUsage.QueryLoggingConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LoggingFeatureSetUsageTests extends AbstractWireSerializingTestCase<LoggingFeatureSetUsage> {

    @Override
    protected Writeable.Reader<LoggingFeatureSetUsage> instanceReader() {
        return LoggingFeatureSetUsage::new;
    }

    @Override
    protected LoggingFeatureSetUsage createTestInstance() {
        return new LoggingFeatureSetUsage(randomQueryLoggingConfig(), randomEsqlLoggingConfig());
    }

    @Override
    protected LoggingFeatureSetUsage mutateInstance(LoggingFeatureSetUsage instance) throws IOException {
        var queryConfig = instance.queryConfig();
        var esqlConfig = instance.esqlConfig();
        return switch (between(0, 6)) {
            case 0 -> {
                var base = queryConfig.base();
                var newBase = new LoggingConfig(!base.enabled(), base.userInfo());
                yield new LoggingFeatureSetUsage(
                    new QueryLoggingConfig(newBase, queryConfig.system(), queryConfig.threshold()),
                    esqlConfig
                );
            }
            case 1 -> {
                var base = queryConfig.base();
                var newBase = new LoggingConfig(base.enabled(), !base.userInfo());
                yield new LoggingFeatureSetUsage(
                    new QueryLoggingConfig(newBase, queryConfig.system(), queryConfig.threshold()),
                    esqlConfig
                );
            }
            case 2 -> new LoggingFeatureSetUsage(
                new QueryLoggingConfig(queryConfig.base(), !queryConfig.system(), queryConfig.threshold()),
                esqlConfig
            );
            case 3 -> {
                String threshold = queryConfig.threshold();
                String newThreshold = threshold == null ? randomAlphaOfLength(5)
                    : randomBoolean() ? null
                    : randomValueOtherThan(threshold, () -> randomAlphaOfLength(8));
                yield new LoggingFeatureSetUsage(
                    new QueryLoggingConfig(queryConfig.base(), queryConfig.system(), newThreshold),
                    esqlConfig
                );
            }
            case 4 -> {
                var base = esqlConfig.base();
                var newBase = new LoggingConfig(!base.enabled(), base.userInfo());
                yield new LoggingFeatureSetUsage(queryConfig, new EsqlLoggingConfig(newBase, esqlConfig.thresholds()));
            }
            case 5 -> {
                var base = esqlConfig.base();
                var newBase = new LoggingConfig(base.enabled(), !base.userInfo());
                yield new LoggingFeatureSetUsage(queryConfig, new EsqlLoggingConfig(newBase, esqlConfig.thresholds()));
            }
            case 6 -> {
                Map<String, String> thresholds = new HashMap<>(esqlConfig.thresholds());
                if (thresholds.isEmpty() || randomBoolean()) {
                    thresholds.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
                } else {
                    String key = randomFrom(thresholds.keySet());
                    thresholds.put(key, randomValueOtherThan(thresholds.get(key), () -> randomAlphaOfLength(8)));
                }
                yield new LoggingFeatureSetUsage(queryConfig, new EsqlLoggingConfig(esqlConfig.base(), thresholds));
            }
            default -> throw new AssertionError("unexpected branch");
        };
    }

    private LoggingConfig randomLoggingConfig() {
        return new LoggingConfig(randomBoolean(), randomBoolean());
    }

    private QueryLoggingConfig randomQueryLoggingConfig() {
        return new QueryLoggingConfig(randomLoggingConfig(), randomBoolean(), randomBoolean() ? null : randomAlphaOfLength(10));
    }

    private EsqlLoggingConfig randomEsqlLoggingConfig() {
        Map<String, String> thresholds = randomMap(0, 5, () -> Tuple.tuple(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        return new EsqlLoggingConfig(randomLoggingConfig(), thresholds);
    }
}
