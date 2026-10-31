/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Provenance;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class OptimizerProfileSerializationTests extends AbstractWireSerializingTestCase<OptimizerProfile> {

    @Override
    protected Writeable.Reader<OptimizerProfile> instanceReader() {
        return OptimizerProfile::new;
    }

    @Override
    protected OptimizerProfile createTestInstance() {
        return randomOptimizerProfile();
    }

    @Override
    protected OptimizerProfile mutateInstance(OptimizerProfile instance) {
        String costModel = instance.costModel();
        List<RunReport> runs = instance.runs();
        int truncatedRuns = instance.truncatedRuns();
        switch (between(0, 2)) {
            case 0 -> costModel = randomValueOtherThan(costModel, () -> randomAlphaOfLength(8));
            case 1 -> runs = randomValueOtherThan(runs, () -> randomList(0, 3, OptimizerProfileSerializationTests::randomRunReport));
            case 2 -> truncatedRuns = randomValueOtherThan(truncatedRuns, () -> between(0, 20));
            default -> throw new AssertionError("unexpected branch");
        }
        return new OptimizerProfile(costModel, runs, truncatedRuns);
    }

    public void testXContentShape() throws IOException {
        CostVector planCost = new CostVector(1.5e9, 2e9, 1024, 2048, 4096, 512);
        DecisionReport decision = new DecisionReport(
            "d1",
            "join_strategy",
            "Join",
            new SourceAnchor("LOOKUP JOIN x ON k", 2, 3),
            "BROADCAST chosen by cost.",
            DecisionMode.COST_BASED,
            new ChosenReport(
                "BROADCAST",
                "source",
                List.of(new CostComponent(CostTerm.EXCHANGE_BROADCAST, Side.RIGHT, new CostVector(1e6, 0, 1024, 0, 0, 512))),
                List.of()
            ),
            new Outcome(OutcomeStatus.APPLIED, null, null, "HashJoinExec", null),
            "any",
            List.of("k{f}#1", "k{f}#2"),
            Provenance.METADATA,
            List.of(
                new NamedEstimate("left_rows", new EstimateView(1000, 1000, Provenance.METADATA)),
                new NamedEstimate("right_bytes", new EstimateView(2.5, Double.NaN, Provenance.DERIVED))
            ),
            List.of(
                new AlternativeReport(
                    "PARTITIONED",
                    AlternativeStatus.LOST_ON_COST,
                    "hash(k{f}#1)",
                    new CostVector(Double.NaN, 3e9, 0, 0, 0, 0),
                    new Delta(1.25, 35.714285),
                    List.of(),
                    List.of(
                        new FitCheck(
                            "broadcast_build_per_node",
                            FitMethod.UPPER_BOUND,
                            new EstimateView(100, 200, Provenance.DERIVED),
                            1024,
                            null,
                            null,
                            true
                        )
                    ),
                    List.of(),
                    null
                )
            ),
            List.of(),
            false,
            "non-finite cost"
        );
        OptimizerProfile profile = new OptimizerProfile(
            OptimizerProfile.COST_MODEL,
            List.of(
                new RunReport(
                    0,
                    Trigger.INITIAL,
                    null,
                    RunStatus.OK,
                    null,
                    new SearchStats(3, 10, 10000, 1, 2_000_000),
                    planCost,
                    List.of(decision),
                    0,
                    null
                )
            ),
            0
        );

        String json = Strings.toString(profile);
        assertThat(json, equalTo(XContentHelper.stripWhitespace("""
            {
              "cost_model": "esql_cbo_v1",
              "score_formula": "latency_seconds + 1e-9 * work_units",
              "runs": [
                {
                  "run": 0,
                  "trigger": "initial",
                  "status": "ok",
                  "search": {
                    "groups": 3,
                    "candidates": 10,
                    "candidate_budget": 10000,
                    "counterfactual_searches": 1,
                    "took_nanos": 2000000
                  },
                  "plan_cost": {
                    "score": 3.5,
                    "latency_nanos": 1500000000,
                    "work_units": 2000000000,
                    "network_in_bytes": 1024,
                    "coordinator_in_bytes": 2048,
                    "peak_node_in_bytes": 4096,
                    "peak_lane_in_bytes": 512
                  },
                  "decisions": [
                    {
                      "id": "d1",
                      "kind": "join_strategy",
                      "node": "Join",
                      "source": { "text": "LOOKUP JOIN x ON k", "line": 2, "column": 3 },
                      "summary": "BROADCAST chosen by cost.",
                      "mode": "cost_based",
                      "chosen": {
                        "name": "BROADCAST",
                        "delivered": "source",
                        "components": [
                          {
                            "term": "exchange_broadcast",
                            "side": "right",
                            "cost": {
                              "score": 0.001,
                              "latency_nanos": 1000000,
                              "work_units": 0,
                              "network_in_bytes": 1024,
                              "coordinator_in_bytes": 0,
                              "peak_node_in_bytes": 0,
                              "peak_lane_in_bytes": 512
                            }
                          }
                        ]
                      },
                      "outcome": { "status": "applied", "physical_node": "HashJoinExec" },
                      "required": "any",
                      "anchor": [ "k{f}#1", "k{f}#2" ],
                      "estimate_basis": "metadata",
                      "inputs": {
                        "left_rows": { "value": 1000, "upper_bound": 1000, "provenance": "metadata" },
                        "right_bytes": { "value": 2.5, "upper_bound": null, "provenance": "derived" }
                      },
                      "alternatives": [
                        {
                          "name": "PARTITIONED",
                          "status": "lost_on_cost",
                          "delivered": "hash(k{f}#1)",
                          "plan_cost": {
                            "score": null,
                            "latency_nanos": null,
                            "work_units": 3000000000,
                            "network_in_bytes": 0,
                            "coordinator_in_bytes": 0,
                            "peak_node_in_bytes": 0,
                            "peak_lane_in_bytes": 0
                          },
                          "delta": { "score": 1.25, "percent": 35.71 },
                          "fit_checks": [
                            {
                              "check": "broadcast_build_per_node",
                              "method": "upper_bound",
                              "demand": { "value": 100, "upper_bound": 200, "provenance": "derived" },
                              "budget_in_bytes": 1024,
                              "passed": true
                            }
                          ]
                        }
                      ],
                      "cost_warning": "non-finite cost"
                    }
                  ]
                }
              ]
            }""")));
        for (String omitted : List.of(
            "\"subplan\"",
            "\"status_reason\"",
            "\"truncated_decisions\"",
            "\"explain\"",
            "\"truncated_runs\"",
            "\"effective\"",
            "\"excluded\"",
            "\"search_anomaly\"",
            "\"other_decisions\"",
            "\"tie_break\"",
            "\"probability\"",
            "\"latency\"",
            "\"network\""
        )) {
            assertThat(json, not(containsString(omitted)));
        }

        ToXContent.Params human = new ToXContent.MapParams(Map.of("human", "true"));
        assertThat(Strings.toString(planCost, human, false, true), equalTo(XContentHelper.stripWhitespace("""
            {
              "score": 3.5,
              "latency_nanos": 1500000000,
              "latency": "1.5s",
              "work_units": 2000000000,
              "network_in_bytes": 1024,
              "network": "1kb",
              "coordinator_in_bytes": 2048,
              "coordinator": "2kb",
              "peak_node_in_bytes": 4096,
              "peak_node": "4kb",
              "peak_lane_in_bytes": 512,
              "peak_lane": "512b"
            }""")));
        String humanJson = Strings.toString(profile, human, false, true);
        assertThat(humanJson, containsString("\"took_nanos\":2000000,\"took\":\"2ms\""));
        // a non-finite latency has no readable form, but the raw field is still present
        assertThat(humanJson, containsString("\"latency_nanos\":null,\"work_units\":3000000000"));
    }

    public static OptimizerProfile randomOptimizerProfile() {
        return new OptimizerProfile(
            randomBoolean() ? OptimizerProfile.COST_MODEL : randomAlphaOfLength(8),
            randomList(0, 3, OptimizerProfileSerializationTests::randomRunReport),
            randomBoolean() ? 0 : between(1, 20)
        );
    }

    private static RunReport randomRunReport() {
        Trigger trigger = randomFrom(Trigger.values());
        return new RunReport(
            between(0, 16),
            trigger,
            trigger == Trigger.AFTER_SUBPLAN ? between(1, 10) : null,
            randomFrom(RunStatus.values()),
            randomBoolean() ? null : randomAlphaOfLength(12),
            randomBoolean() ? null : randomSearchStats(),
            randomBoolean() ? null : randomCostVector(),
            randomList(0, 3, OptimizerProfileSerializationTests::randomDecisionReport),
            randomBoolean() ? 0 : between(1, 10),
            randomBoolean() ? null : randomAlphaOfLength(20)
        );
    }

    private static SearchStats randomSearchStats() {
        return new SearchStats(between(0, 100), between(0, 10000), between(1, 100000), between(0, 32), randomNonNegativeLong());
    }

    private static DecisionReport randomDecisionReport() {
        return new DecisionReport(
            "d" + between(1, 64),
            randomFrom("join_strategy", "aggregate_distribution"),
            randomAlphaOfLength(6),
            randomBoolean() ? null : new SourceAnchor(randomAlphaOfLength(10), between(1, 100), between(1, 100)),
            randomAlphaOfLength(20),
            randomFrom(DecisionMode.values()),
            new ChosenReport(
                randomAlphaOfLength(6),
                randomAlphaOfLength(6),
                randomList(0, 3, OptimizerProfileSerializationTests::randomCostComponent),
                randomList(0, 2, OptimizerProfileSerializationTests::randomFitCheck)
            ),
            randomBoolean() ? null : randomOutcome(),
            randomFrom("any", "single", "broadcast", "hash(k{f}#1)"),
            randomList(0, 3, () -> randomAlphaOfLength(5)),
            randomFrom(Provenance.values()),
            randomList(0, 4, () -> new NamedEstimate(randomAlphaOfLength(8), randomEstimateView())),
            randomList(0, 3, OptimizerProfileSerializationTests::randomAlternativeReport),
            randomList(0, 2, () -> new Exclusion(randomAlphaOfLength(6), randomFrom(ExclusionCode.values()), randomOptionalString())),
            randomBoolean(),
            randomOptionalString()
        );
    }

    private static Outcome randomOutcome() {
        return new Outcome(
            randomFrom(OutcomeStatus.values()),
            randomOptionalString(),
            randomOptionalString(),
            randomOptionalString(),
            randomOptionalString()
        );
    }

    private static AlternativeReport randomAlternativeReport() {
        return new AlternativeReport(
            randomAlphaOfLength(6),
            randomFrom(AlternativeStatus.values()),
            randomBoolean() ? null : randomAlphaOfLength(6),
            randomBoolean() ? null : randomCostVector(),
            randomBoolean() ? null : new Delta(randomCostDouble(), randomCostDouble()),
            randomList(0, 3, OptimizerProfileSerializationTests::randomCostComponent),
            randomList(0, 2, OptimizerProfileSerializationTests::randomFitCheck),
            randomList(0, 2, () -> new DecisionRef("d" + between(1, 64), randomAlphaOfLength(6))),
            randomBoolean() ? null : new TieBreak(randomFrom(TieBreakRule.values()), randomDoubleBetween(0, 5, true))
        );
    }

    private static CostComponent randomCostComponent() {
        return new CostComponent(randomFrom(CostTerm.values()), randomBoolean() ? null : randomFrom(Side.values()), randomCostVector());
    }

    private static FitCheck randomFitCheck() {
        boolean probability = randomBoolean();
        return new FitCheck(
            randomFrom("materialize_build_bytes", "broadcast_build_per_node", "coordinator_aggregate_state"),
            probability ? FitMethod.PROBABILITY : FitMethod.UPPER_BOUND,
            randomEstimateView(),
            randomNonNegativeLong(),
            probability ? randomDoubleBetween(0, 1, true) : null,
            probability ? randomDoubleBetween(0, 1, true) : null,
            randomBoolean()
        );
    }

    private static EstimateView randomEstimateView() {
        return new EstimateView(randomCostDouble(), randomCostDouble(), randomFrom(Provenance.values()));
    }

    private static CostVector randomCostVector() {
        return new CostVector(
            randomCostDouble(),
            randomCostDouble(),
            randomCostDouble(),
            randomCostDouble(),
            randomCostDouble(),
            randomCostDouble()
        );
    }

    private static double randomCostDouble() {
        if (rarely()) {
            return randomFrom(Double.NaN, Double.POSITIVE_INFINITY);
        }
        return randomBoolean() ? randomLongBetween(0, 1L << 40) : randomDoubleBetween(0, 1e12, true);
    }

    private static String randomOptionalString() {
        return randomBoolean() ? null : randomAlphaOfLength(10);
    }
}
