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

    public void testBinaryWithDifferentSelectors() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1m result=(sum(avg_over_time(network.cost[1m]) + avg_over_time(network.cost[10m])))")
            .transportVersion(TransportVersion.current())
            .run();
    }

    public void testInstantQueryScalarTimeFn() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL instant query support", EsqlCapabilities.Cap.PROMQL_INSTANT_QUERY.isEnabled());
        builder("PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(time())").transportVersion(TransportVersion.current()).run();
    }

    public void testTopk() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL topk support", EsqlCapabilities.Cap.PROMQL_TOPK.isEnabled());
        builder("PROMQL index=k8s step=1h result=(topk(3, network.bytes_in))").transportVersion(TransportVersion.current()).run();
    }

    public void testTopkByGrouping() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL topk support", EsqlCapabilities.Cap.PROMQL_TOPK.isEnabled());
        builder("PROMQL index=k8s step=1h result=(topk(2, network.bytes_in) by (pod))").transportVersion(TransportVersion.current()).run();
    }

    public void testTopkOverSumBy() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL topk support", EsqlCapabilities.Cap.PROMQL_TOPK.isEnabled());
        assumeTrue("requires fix for topk over aggregated vectors", EsqlCapabilities.Cap.FIX_PROMQL_TOPK_OVER_AGGREGATE.isEnabled());
        builder("PROMQL index=k8s step=1h result=(topk(2, sum by (pod) (network.bytes_in)))").transportVersion(TransportVersion.current())
            .run();
    }
}
