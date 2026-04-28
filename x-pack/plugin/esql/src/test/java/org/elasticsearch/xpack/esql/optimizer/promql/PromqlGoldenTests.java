/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

/**
 * Golden tests for PromQL to ESQL plan translation.
 */
public class PromqlGoldenTests extends GoldenTestCase {

    public void testSimpleInstantSelector() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1h network.bytes_in").transportVersion(TransportVersion.current()).run();
    }

    public void testAdditionScalarScalar() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1h sum=(1+1)").transportVersion(TransportVersion.current()).run();
    }

    public void testMultiplicationMetricScalar() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1h network_in_bits=(network.total_bytes_in * 8)").transportVersion(TransportVersion.current()).run();
    }

    public void testMultiplicationAcrossSeriesScalar() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("""
            PROMQL index=k8s step=1h max_bits=(
              max(network.total_bytes_in) * 8
            )""").transportVersion(TransportVersion.current()).run();
    }

    public void testFirstOverTimeAllValueTypes() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=10m events=(sum by (pod) (first_over_time(events_received[10m])))").transportVersion(
            TransportVersion.current()
        ).run();
    }

    public void testPromqlSourceWithGrok() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires URI_PARTS command", EsqlCapabilities.Cap.URI_PARTS_COMMAND.isEnabled());
        builder("""
            PROMQL index=k8s-downsampled step=1h oYJdEiiJ=(network.bytes_in{cluster!="qa",pod!="two"})
            | GROK _timeseries "%{WORD:zEyDkwmbYa} %{WORD:step} %{WORD:step}"
            | URI_PARTS parts = _timeseries
            | DROP _timeseries, oYJdEiiJ, step, zEyDkwmbYa
            | LIMIT 1""").transportVersion(TransportVersion.current()).run();
    }

    public void testImplicitLastOverTimeOfLong() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1m bytes=(avg by (cluster) (network.bytes_in))").transportVersion(TransportVersion.current()).run();
    }

    public void testCaseInsensitivityAggregator() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1h bytes=(SUM by (pod) (network.bytes_in))").transportVersion(TransportVersion.current()).run();
    }
}
