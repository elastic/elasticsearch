/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.esql.execution.sequence.SequenceSourceStatus;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/**
 * Checks chosen scalar expansion values and authentic physical originals through real execution and consumers.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceRetainedExpansionIT extends AbstractSequenceIntegTestCase {
    private static final String A = "p3-proof-a";
    private static final String B = "p3-proof-b";
    private static final String QUOTED = "$$sequence_original$1";
    private static final String PREFIX = "p3-retained-proof-";
    private static final Instant BASE = Instant.parse("2026-11-12T00:00:00.000000001Z");
    private static final TimeValue TIMEOUT = TimeValue.timeValueMinutes(2);
    private static final TimeValue CLEANUP_TIMEOUT = TimeValue.timeValueSeconds(60);
    private static final Object MISSING = new Object();
    private static final AtomicInteger LABELS = new AtomicInteger();
    private MockBlockFactory consumerBlocks;
    private OwnedResponse active;
    private Throwable cleanupViolation;
    private String coordinator;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(TrackingPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings other) {
        return Settings.builder()
            .put(super.nodeSettings(ordinal, other))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, "0b")
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, "0b")
            .build();
    }

    /** Track native pages independently of the reference consumer's deserialized blocks. */
    public static class TrackingPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }
    }

    /** Allocate only the private reference-reader breaker; no fixture namespace is touched before its ownership check. */
    @Before
    public void prepareConsumer() {
        consumerBlocks = new MockBlockFactory(
            BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(64)).withCircuitBreaking())
        );
        coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
    }

    /** Both primary choices preserve expanded scalar keys and authentic original events. */
    public void testBothPrimaryChoicesKeepExpandedOriginals() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            for (int primary : List.of(1, 2)) {
                coreCheck(query(coreQuery(primary, 10)), docs, primary);
            }
        });
    }

    /** The same physical originals under two scalar keys must survive actual row/columnar JSON and wire consumers. */
    public void testExpandedKeysKeepBothAuthenticOriginals() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            for (int primary : List.of(1, 2))
                for (boolean columnar : List.of(false, true)) {
                    Result result = query(coreQuery(primary, 10), 1, columnar);
                    coreCheck(result, docs, primary);
                    assertEquals(2L, result.status.completedMatches());
                }
        });
    }

    /** Explicit duplicated computed keys must not multiply a per-key physical match. */
    public void testDuplicateExpandedKeysDoNotMultiplyMatches() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            for (int primary : List.of(1, 2)) {
                String stage = "EVAL keys=MV_APPEND(keys,keys) | MV_EXPAND keys | KEEP keys,actor";
                Result result = query(
                    retained(
                        sequence("keys", primary, stage, stage),
                        "EVAL other=MEMBER(" + (3 - primary) + ",actor) | KEEP keys,actor,other",
                        "keys",
                        10
                    )
                );
                coreCheck(result, docs, primary);
                assertEquals(2L, result.status.completedMatches());
            }
        });
    }

    /** A same-index event eligible for both stages cannot serve as its own strict predecessor, even through duplicate keys. */
    public void testExpandedDuplicatesCannotSelfChain() throws Exception {
        Doc first = core().getFirst();
        Doc last = document(A, "B", 1, List.of("alpha", "beta"), "second");
        String stage = "EVAL keys=MV_APPEND(keys,keys) | MV_EXPAND keys | KEEP keys,actor";
        String text = retained(sequence("keys", 1, A, stage, A, stage), "EVAL other=MEMBER(2,actor) | KEEP keys,actor,other", "keys", 10);
        withDocs(List.of(first), () -> check(query(text), 1, List.of("keys", "actor", "other"), keywords(3), List.of()));
        withDocs(
            List.of(first, last),
            () -> check(
                query(text),
                1,
                List.of("keys", "actor", "other"),
                keywords(3),
                List.of(
                    expected("alpha", cells("alpha", "first", "second"), first, last),
                    expected("beta", cells("beta", "first", "second"), first, last)
                )
            )
        );
    }

    /** Unpinned unequal non-key rows challenge which scalar survives same-key dedup, including explicit repeated candidates. */
    public void testNonKeySelectedPositionSurvivesDirectAndForcedReplay() throws Exception {
        verifyNonKey(false, false, true);
        verifyNonKey(false, true, true);
    }

    /** Ordinary output independently checks the selected scalar under same-key expansion deduplication. */
    public void testOrdinaryNonKeyPositionControl() throws Exception {
        verifyNonKey(false, true, false);
        verifyNonKey(true, true, false);
    }

    /** A predicate-pinned non-key scalar keeps its original array; no general first-value API is introduced. */
    public void testPinnedNonKeyExpansionKeepsFullOriginalArray() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            String stage = "MV_EXPAND tags | WHERE tags==\"tag-z\" | KEEP key,tags";
            for (int primary : List.of(1, 2))
                check(
                    query(
                        retained(
                            sequence("key", primary, stage, stage),
                            "EVAL other=MEMBER(" + (3 - primary) + ",tags) | KEEP key,tags,other",
                            "key",
                            10
                        )
                    ),
                    primary,
                    List.of("key", "tags", "other"),
                    keywords(3),
                    List.of(expected("shared", cells("shared", "tag-z", "tag-z"), docs.get(0), docs.get(1)))
                );
        });
    }

    /** Keys x unequal non-key rows exercise a chosen flattened expansion position greater than zero and repeated same keys. */
    public void testChainedExpansionKeepsNonzeroSelectedPosition() throws Exception {
        verifyNonKey(true, true, true);
    }

    /** Metadata-shaped processed targets must remain separate from the genuine protected index/id/SOURCE slots. */
    public void testMetadataShapedExpansionTargetsKeepGenuineOriginals() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            for (String target : List.of("_id", "_index", "_source"))
                for (int primary : List.of(1, 2)) {
                    String stage = "MV_EXPAND keys | EVAL "
                        + target
                        + "=MV_APPEND(actor,actor) | MV_EXPAND "
                        + target
                        + " | KEEP keys,"
                        + target;
                    Result result = query(
                        retained(
                            sequence("keys", primary, stage, stage),
                            "EVAL other=MEMBER(" + (3 - primary) + "," + target + ") | KEEP keys," + target + ",other",
                            "keys",
                            10
                        )
                    );
                    String actor = primary == 1 ? "first" : "second", other = primary == 1 ? "second" : "first";
                    check(
                        result,
                        primary,
                        List.of("keys", target, "other"),
                        keywords(3),
                        List.of(
                            expected("alpha", cells("alpha", actor, other), docs.get(0), docs.get(1)),
                            expected("beta", cells("beta", actor, other), docs.get(0), docs.get(1))
                        )
                    );
                }
        });
    }

    /** Real scalar metadata expansion follows its existing ordinary behavior; SOURCE is never given a new coercion rule. */
    public void testGenuineMetadataTargetsFollowOrdinaryControl() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            for (String target : List.of("_index", "_id", "_source")) {
                String stage = "MV_EXPAND " + target + " | KEEP key";
                String sequence = sequence("key", 1, A + " METADATA " + target, stage, B + " METADATA " + target, stage);
                assertEquals(List.of(cells("shared")), query(sequence + " | KEEP key | LIMIT 10").values);
                check(
                    query(retained(sequence, "KEEP key", "key", 10)),
                    1,
                    List.of("key"),
                    keywords(1),
                    List.of(expected("shared", cells("shared"), docs.get(0), docs.get(1)))
                );
            }
        });
    }

    /** Quoted compiler-shaped/mapped targets and a real diagnostic-name collision must be resolved by binding IDs. */
    public void testQuotedAndDiagnosticNameCollisionsKeepOriginalBindings() throws Exception {
        List<Doc> docs = core().stream().map(doc -> field(doc, QUOTED, List.of("mapped-z", "mapped-a"))).toList();
        withDocs(docs, () -> {
            for (int primary : List.of(1, 2))
                for (boolean mapped : List.of(false, true)) {
                    String value = mapped ? "mapped-z" : "z";
                    String prepare = mapped
                        ? ""
                        : "DROP `" + QUOTED + "` | EVAL value=MV_APPEND(\"z\",\"a\") | RENAME value AS `" + QUOTED + "` | ";
                    String stage = "MV_EXPAND keys | "
                        + prepare
                        + "MV_EXPAND `"
                        + QUOTED
                        + "` | WHERE `"
                        + QUOTED
                        + "`==\""
                        + value
                        + "\" | KEEP keys,`"
                        + QUOTED
                        + "`";
                    Result result = query(
                        retained(
                            sequence("keys", primary, stage, stage),
                            "EVAL other=MEMBER("
                                + (3 - primary)
                                + ",`"
                                + QUOTED
                                + "`), `$$sequence_key$1`=\"ordinary-key\","
                                + " `$$sequence_member$1`=\"ordinary-member\", member_1_id=\"ordinary-id\""
                                + " | KEEP keys,`"
                                + QUOTED
                                + "`,other,`$$sequence_key$1`,`$$sequence_member$1`,member_1_id",
                            "keys",
                            10
                        )
                    );
                    check(
                        result,
                        primary,
                        List.of("keys", QUOTED, "other", "$$sequence_key$1", "$$sequence_member$1", "member_1_id"),
                        keywords(6),
                        List.of(
                            expected(
                                "alpha",
                                cells("alpha", value, value, "ordinary-key", "ordinary-member", "ordinary-id"),
                                docs.get(0),
                                docs.get(1)
                            ),
                            expected(
                                "beta",
                                cells("beta", value, value, "ordinary-key", "ordinary-member", "ordinary-id"),
                                docs.get(0),
                                docs.get(1)
                            )
                        )
                    );
                }
        });
    }

    /** Null/empty/missing correlation values remain unmatchable, while scalar input is one authentic event membership. */
    public void testNullEmptyMissingAndScalarExpandedKeys() throws Exception {
        for (Object keys : Arrays.asList(null, List.of(), MISSING, "alpha")) {
            List<Doc> docs = List.of(document(A, "A", 0, keys, "first"), document(B, "B", 1, "alpha", "second"));
            withDocs(docs, () -> {
                for (int primary : List.of(1, 2)) {
                    List<Expected> wanted = keys instanceof String
                        ? List.of(
                            expected(
                                "alpha",
                                cells("alpha", primary == 1 ? "first" : "second", primary == 1 ? "second" : "first"),
                                docs.get(0),
                                docs.get(1)
                            )
                        )
                        : List.of();
                    check(query(coreQuery(primary, 10)), primary, List.of("keys", "actor", "other"), keywords(3), wanted);
                }
            });
        }
    }

    /** Non-key null/empty/missing values preserve the ordinary null row; the source keeps their distinct original shapes. */
    public void testNonKeyNullEmptyMissingAndScalarExpansion() throws Exception {
        for (Object tags : Arrays.asList(null, List.of(), MISSING, "one")) {
            List<Doc> docs = core().stream().map(doc -> tags == MISSING ? without(doc, "tags") : field(doc, "tags", tags)).toList();
            withDocs(docs, () -> {
                String stage = "MV_EXPAND tags | KEEP key,tags";
                String sequence = sequence("key", 1, stage, stage);
                Object wanted = tags instanceof String ? tags : null;
                Result ordinary = query(sequence + " | EVAL other=MEMBER(2,tags) | KEEP key,tags,other | LIMIT 10");
                assertEquals(List.of(cells("shared", wanted, wanted)), ordinary.values);
                check(
                    query(retained(sequence, "EVAL other=MEMBER(2,tags) | KEEP key,tags,other", "key", 10)),
                    1,
                    List.of("key", "tags", "other"),
                    keywords(3),
                    List.of(expected("shared", cells("shared", wanted, wanted), docs.get(0), docs.get(1)))
                );
            });
        }
    }

    /** A predicate on the new expanded ID must not become a predicate on the original multi-valued source field. */
    public void testHoistedFiltersPreserveExpandedIdentity() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            String stage = "MV_EXPAND keys | WHERE actor!=\"discard\" | WHERE keys!=\"beta\""
                + " | EVAL value=CONCAT(actor,\"/\",keys) | KEEP keys,value";
            for (int primary : List.of(1, 2))
                check(
                    query(
                        retained(
                            sequence("keys", primary, stage, stage),
                            "EVAL other=MEMBER(" + (3 - primary) + ",value) | KEEP keys,value,other",
                            "keys",
                            10
                        )
                    ),
                    primary,
                    List.of("keys", "value", "other"),
                    keywords(3),
                    List.of(
                        expected(
                            "alpha",
                            cells("alpha", (primary == 1 ? "first" : "second") + "/alpha", (primary == 1 ? "second" : "first") + "/alpha"),
                            docs.get(0),
                            docs.get(1)
                        )
                    )
                );
        });
    }

    /** Projection/rename and parsing after expansion cannot sever protected originals or the selected scalar key. */
    public void testProjectionPruningAndParsingKeepExpandedOriginals() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            for (boolean parsing : List.of(false, true))
                for (int primary : List.of(1, 2)) {
                    String stage = parsing
                        ? "MV_EXPAND keys | EVAL raw=CONCAT(keys,\":\",actor) | DISSECT raw \"%{chosen}:%{payload}\""
                            + " | DROP raw,actor | RENAME payload AS actor | KEEP chosen,actor"
                        : "EVAL expanded=keys | MV_EXPAND expanded | RENAME expanded AS chosen"
                            + " | EVAL unused=CONCAT(actor,\"/unused\") | DROP keys,unused | KEEP chosen,actor";
                    Result result = query(
                        retained(
                            sequence("chosen", primary, stage, stage),
                            "EVAL other=MEMBER(" + (3 - primary) + ",actor) | KEEP chosen,actor,other",
                            "chosen",
                            10
                        )
                    );
                    String first = primary == 1 ? "first" : "second", last = primary == 1 ? "second" : "first";
                    check(
                        result,
                        primary,
                        List.of("chosen", "actor", "other"),
                        keywords(3),
                        List.of(
                            expected("alpha", cells("alpha", first, last), docs.get(0), docs.get(1)),
                            expected("beta", cells("beta", first, last), docs.get(0), docs.get(1))
                        )
                    );
                }
        });
    }

    /** NULLIFY applies to a genuinely absent expansion input and does not replace authentic source slots with nulls. */
    public void testNullifyMissingExpansionInput() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            String prefix = "SET unmapped_fields=\"nullify\"; ";
            String stage = "EVAL pick=absent_optional | MV_EXPAND pick | KEEP key,pick";
            check(
                query(prefix + retained(sequence("key", 1, stage, stage), "EVAL other=MEMBER(2,pick) | KEEP key,pick,other", "key", 10)),
                1,
                List.of("key", "pick", "other"),
                List.of("keyword", "null", "null"),
                List.of(expected("shared", cells("shared", null, null), docs.get(0), docs.get(1)))
            );
            String missingKey = "EVAL missing_key=absent_optional | MV_EXPAND missing_key | KEEP missing_key,actor";
            check(
                query(
                    prefix + retained(
                        sequence("missing_key", 1, missingKey, missingKey),
                        "EVAL other=MEMBER(2,actor) | KEEP missing_key,actor,other",
                        "missing_key",
                        10
                    )
                ),
                1,
                List.of("missing_key", "actor", "other"),
                List.of("null", "keyword", "keyword"),
                List.of()
            );
        });
    }

    /** Projected-away mapped/generated expanded attributes must not reappear as nullified member demands. */
    public void testNullifyDoesNotRestoreRemovedExpandedOutput() throws Exception {
        withDocs(core(), () -> {
            for (boolean generated : List.of(false, true)) {
                String target = generated ? "pick" : "tags";
                String stage = (generated ? "EVAL pick=tags | " : "") + "MV_EXPAND " + target + " | DROP " + target + " | KEEP key,actor";
                String text = "SET unmapped_fields=\"nullify\"; "
                    + retained(sequence("key", 1, stage, stage), "EVAL observed=MEMBER(2," + target + ") | KEEP key,observed", "key", 10);
                Exception failure = expectThrows(Exception.class, () -> query(text));
                assertThat(failure.getMessage(), containsString("Unknown column"));
                assertThat(failure.getMessage(), containsString(target));
            }
        });
    }

    /** The last physical candidate on page two must retain the exact pair of complete originals. */
    public void testCandidatePageBoundariesKeepExpandedOriginals() throws Exception {
        for (int count : List.of(255, 256, 257)) {
            List<Doc> docs = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                docs.add(document(A, "A" + i, i, List.of(key(i)), "first"));
                docs.add(document(B, "B" + i, 1000 + i, List.of(key(i)), "second"));
            }
            withDocs(docs, () -> {
                for (int primary : List.of(1, 2)) {
                    List<Expected> expected = new ArrayList<>();
                    for (int i = 0; i < count; i++)
                        expected.add(
                            expected(
                                key(i),
                                cells(key(i), primary == 1 ? "first" : "second", primary == 1 ? "second" : "first"),
                                docs.get(2 * i),
                                docs.get(2 * i + 1)
                            )
                        );
                    check(query(coreQuery(primary, 10000)), primary, List.of("keys", "actor", "other"), keywords(3), expected);
                }
            });
        }
    }

    /** Pragma variations and match batches preserve repeated originals; the separate 5001-value test proves actual expand chunks. */
    public void testPragmaVariationsAndPayloadBatchesKeepSelectedExpansions() throws Exception {
        for (int count : List.of(63, 64, 65, 150)) {
            List<String> keys = new ArrayList<>();
            for (int i = count - 1; i >= 0; i--)
                keys.add(key(i));
            List<Doc> docs = List.of(document(A, "A", 0, keys, "first"), document(B, "B", 1, keys, "second"));
            withDocs(docs, () -> {
                for (int page : List.of(1, 2, 128))
                    for (boolean replay : List.of(false, true))
                        for (int primary : List.of(1, 2)) {
                            String stage = "MV_EXPAND keys | EVAL value=CONCAT(actor,\"/\",keys)"
                                + (replay ? "" : " | WHERE LENGTH(value)>0")
                                + " | KEEP keys,value";
                            List<Expected> wanted = new ArrayList<>();
                            for (int i = 0; i < count; i++)
                                wanted.add(
                                    expected(
                                        key(i),
                                        cells(
                                            key(i),
                                            (primary == 1 ? "first" : "second") + "/" + key(i),
                                            (primary == 1 ? "second" : "first") + "/" + key(i)
                                        ),
                                        docs.get(0),
                                        docs.get(1)
                                    )
                                );
                            check(
                                query(
                                    retained(
                                        sequence("keys", primary, stage, stage),
                                        "EVAL other=MEMBER(" + (3 - primary) + ",value) | KEEP keys,value,other",
                                        "keys",
                                        10000
                                    ),
                                    page,
                                    false
                                ),
                                primary,
                                List.of("keys", "value", "other"),
                                keywords(3),
                                wanted
                            );
                        }
            });
        }
    }

    /** The planner emits expansion chunks of 5000; 5001 actual values cross that boundary before the predicate leaves one row. */
    public void testActualExpansionChunkBoundaryKeepsFinalSelectedScalar() throws Exception {
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < 5001; i++)
            tags.add("tag-" + (10000 + i));
        List<Doc> docs = core().stream().map(doc -> field(doc, "tags", tags)).toList();
        withDocs(docs, () -> {
            for (boolean replay : List.of(false, true))
                for (int primary : List.of(1, 2)) {
                    String stage = "MV_EXPAND tags | WHERE tags==\"tag-15000\" | EVAL value="
                        + (replay ? "CONCAT(tags,\"/replay\")" : "tags")
                        + " | KEEP key,value";
                    String member = "MEMBER(" + (3 - primary) + ",value)";
                    String present = replay ? "REPLACE(value,\"/replay\",\"\")" : "value";
                    String other = replay ? "REPLACE(" + member + ",\"/replay\",\"\")" : member;
                    check(
                        query(
                            retained(
                                sequence("key", primary, stage, stage),
                                "EVAL present=" + present + ",other=" + other + " | KEEP key,present,other",
                                "key",
                                10
                            ),
                            1,
                            false
                        ),
                        primary,
                        List.of("key", "present", "other"),
                        keywords(3),
                        List.of(expected("shared", cells("shared", "tag-15000", "tag-15000"), docs.get(0), docs.get(1)))
                    );
                }
        });
    }

    /** Outer limit preserves the chosen row/body and closes all unused work; it need not visit every possible match. */
    public void testLimitClosesUnusedExpansions() throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            for (int primary : List.of(1, 2))
                check(
                    query(coreQuery(primary, 1), 1, false),
                    primary,
                    List.of("keys", "actor", "other"),
                    keywords(3),
                    List.of(
                        expected(
                            "alpha",
                            cells("alpha", primary == 1 ? "first" : "second", primary == 1 ? "second" : "first"),
                            docs.get(0),
                            docs.get(1)
                        )
                    )
                );
        });
    }

    /** Cancel actual parents with pending expanded candidates and with an already selected original awaiting second payload. */
    public void testCancellationDrainsExpansionAndReplay() throws Exception {
        withDocs(core(), () -> {
            String stage = "MV_EXPAND keys | EVAL value=CONCAT(actor,\"/\",keys) | KEEP keys,value";
            String text = retained(sequence("keys", 1, stage, stage), "EVAL other=MEMBER(2,value) | KEEP keys,value,other", "keys", 10);
            for (GateMode mode : GateMode.values()) {
                Exception failure = expectThrows(Exception.class, () -> query(text, 1, false, mode, true, false));
                assertNotNull(ExceptionsHelper.unwrap(failure, TaskCancelledException.class));
            }
        });
    }

    /** A real generous-limit fanout succeeds; lowering the breaker at the selected-payload gate must fail and release everything. */
    public void testWideFanoutBreakerReleasesOriginals() throws Exception {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 512; i++)
            keys.add(key(i));
        List<Doc> docs = List.of(
            field(document(A, "A", 0, keys, "first"), "pad", "a".repeat(8192)),
            field(document(B, "B", 1, keys, "second"), "pad", "b".repeat(8192))
        );
        withDocs(docs, () -> {
            List<Expected> wanted = new ArrayList<>();
            for (String key : keys)
                wanted.add(expected(key, cells(key, "first", "second"), docs.get(0), docs.get(1)));
            check(query(coreQuery(1, 10000)), 1, List.of("keys", "actor", "other"), keywords(3), wanted);
            Exception failure = expectThrows(Exception.class, () -> query(coreQuery(1, 10000), 1, false, GateMode.PAYLOAD, false, true));
            assertNotNull(ExceptionsHelper.unwrap(failure, CircuitBreakingException.class));
            coreCheckAfterFanoutRecovery(docs);
        });
    }

    /** Only stage-local expansion is proposed: retained unions/outer expansion/timestamp overwrites/still-MV keys stay rejected. */
    public void testOuterEvidenceAndScalarBoundariesStayNarrow() throws Exception {
        withDocs(core(), () -> {
            String stage = "MV_EXPAND keys | KEEP keys,actor";
            String seq = sequence("keys", 1, stage, stage);
            Exception outer = expectThrows(Exception.class, () -> query(seq + " | MV_EXPAND keys | LIMIT 10 | RETURN MATCHES"));
            assertThat(outer.getMessage(), containsString("outside the sequence output prototype"));
            Exception union = expectThrows(Exception.class, () -> query("FROM (" + seq + "), (" + seq + ") | LIMIT 10 | RETURN MATCHES"));
            assertThat(union.getMessage(), containsString("ordinary union"));
            String overwritten = "EVAL @timestamp=NOW() | MV_EXPAND keys | KEEP keys,actor";
            Exception time = expectThrows(
                Exception.class,
                () -> query(retained(sequence("keys", 1, overwritten, stage), "KEEP keys", "keys", 10))
            );
            assertThat(time.getMessage(), containsString("cannot overwrite the source [@timestamp]"));
            String unexpanded = sequence("keys", 1, "KEEP keys,actor", "KEEP keys,actor");
            Exception mv = expectThrows(Exception.class, () -> query(retained(unexpanded, "KEEP keys", "keys", 10)));
            assertThat(mv.getMessage(), containsString("use MV_EXPAND inside the stage"));
        });
    }

    private static String coreQuery(int primary, int limit) {
        String stage = "MV_EXPAND keys | KEEP keys,actor";
        return retained(
            sequence("keys", primary, stage, stage),
            "EVAL other=MEMBER(" + (3 - primary) + ",actor) | KEEP keys,actor,other",
            "keys",
            limit
        );
    }

    private static void coreCheck(Result result, List<Doc> docs, int primary) {
        String actor = primary == 1 ? "first" : "second", other = primary == 1 ? "second" : "first";
        check(
            result,
            primary,
            List.of("keys", "actor", "other"),
            keywords(3),
            List.of(
                expected("alpha", cells("alpha", actor, other), docs.get(0), docs.get(1)),
                expected("beta", cells("beta", actor, other), docs.get(0), docs.get(1))
            )
        );
    }

    private void verifyNonKey(boolean expandedKeys, boolean duplicateValues, boolean capture) throws Exception {
        List<Doc> docs = core();
        withDocs(docs, () -> {
            String key = expandedKeys ? "keys" : "key";
            String prefix = expandedKeys
                ? "EVAL raw_key=MV_APPEND(MV_APPEND(\"alpha\",\"beta\"),MV_APPEND(\"ALPHA\",\"BETA\"))"
                    + " | MV_EXPAND raw_key | EVAL keys=TO_LOWER(raw_key) | "
                : "";
            String stem = expandedKeys ? "CONCAT(actor,\"/\",raw_key)" : "actor";
            String pair = "MV_APPEND(CONCAT(" + stem + ",\"/z\"),CONCAT(" + stem + ",\"/a\"))";
            if (duplicateValues) pair = "MV_APPEND(" + pair + "," + pair + ")";
            for (boolean replay : List.of(false, true))
                for (int primary : List.of(1, 2))
                    for (int page : List.of(1, 128)) {
                        String stage = prefix
                            + "EVAL pick="
                            + pair
                            + " | MV_EXPAND pick | EVAL value="
                            + (replay ? "CONCAT(pick,\"/replay\")" : "pick")
                            + " | KEEP "
                            + key
                            + ",value";
                        String seq = sequence(key, primary, stage, stage);
                        String first = replay ? "REPLACE(value,\"/replay\",\"\")" : "value";
                        String member = "MEMBER(" + (3 - primary) + ",value)";
                        String other = replay ? "REPLACE(" + member + ",\"/replay\",\"\")" : member;
                        String typedQuery = retained(
                            seq,
                            "EVAL present=" + first + ",other=" + other + " | KEEP " + key + ",present,other",
                            key,
                            10
                        );
                        if (capture == false) typedQuery = typedQuery.substring(0, typedQuery.lastIndexOf(" | RETURN MATCHES"));
                        Result typed = query(typedQuery, page, false);
                        List<Expected> wanted = new ArrayList<>();
                        for (String matchKey : expandedKeys ? List.of("alpha", "beta") : List.of("shared")) {
                            String suffix = expandedKeys ? "/" + matchKey + "/z" : "/z";
                            wanted.add(
                                expected(
                                    matchKey,
                                    cells(
                                        matchKey,
                                        (primary == 1 ? "first" : "second") + suffix,
                                        (primary == 1 ? "second" : "first") + suffix
                                    ),
                                    docs.get(0),
                                    docs.get(1)
                                )
                            );
                        }
                        if (capture) check(typed, primary, List.of(key, "present", "other"), keywords(3), wanted);
                        else {
                            assertEquals(List.of(key, "present", "other"), typed.names);
                            assertEquals(keywords(3), typed.types);
                            assertEquals(wanted.stream().map(Expected::ordinary).toList(), typed.values);
                        }
                        assertEquals(wanted.size(), typed.status.completedMatches());
                        Result count = query(seq + " | STATS matched=COUNT(*) | LIMIT 1", page, false);
                        assertEquals(List.of(cells((long) wanted.size())), count.values);
                        assertEquals(0L, count.status.payloadSearchRequests());
                        assertEquals(count.status.candidateSearchRequests(), typed.status.candidateSearchRequests());
                        assertEquals(count.status.candidateRowsFetched(), typed.status.candidateRowsFetched());
                        if (replay) assertTrue("actual replay drivers must run", typed.status.stageDrivers() > count.status.stageDrivers());
                        else assertEquals(
                            "direct selected cell must need no replay driver",
                            count.status.stageDrivers(),
                            typed.status.stageDrivers()
                        );
                        emit(
                            Map.of(
                                "non_key_position_proof",
                                true,
                                "expanded_keys",
                                expandedKeys,
                                "duplicate_values",
                                duplicateValues,
                                "retained",
                                capture,
                                "forced_replay",
                                replay,
                                "primary",
                                primary,
                                "page",
                                page,
                                "ordinary_rows",
                                capture ? typed.decoded.rows.stream().map(SequenceMatchDescription.Row::ordinary).toList() : typed.values,
                                "typed_stage_drivers",
                                typed.status.stageDrivers(),
                                "count_stage_drivers",
                                count.status.stageDrivers()
                            )
                        );
                    }
        });
    }

    private void coreCheckAfterFanoutRecovery(List<Doc> docs) throws Exception {
        Result result = query(coreQuery(1, 1));
        check(
            result,
            1,
            List.of("keys", "actor", "other"),
            keywords(3),
            List.of(expected(key(0), cells(key(0), "first", "second"), docs.get(0), docs.get(1)))
        );
    }

    private static Doc without(Doc original, String name) {
        Map<String, Object> source = new LinkedHashMap<>(original.source);
        source.remove(name);
        return new Doc(original.index, original.id, Collections.unmodifiableMap(source));
    }

    private static String key(int value) {
        return "k-" + (10000 + value);
    }

    private static Doc document(String index, String id, long millis, Object keys, String actor) {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("@timestamp", BASE.plusMillis(millis).toString());
        source.put("key", "shared");
        if (keys != MISSING) source.put("keys", keys);
        source.put("actor", actor);
        source.put("tags", List.of("tag-z", "tag-a", "tag-z"));
        source.put("body", Map.of("marker", id, "values", List.of(3, 1, 3)));
        return new Doc(index, id, Collections.unmodifiableMap(source));
    }

    private static Doc field(Doc original, String name, Object value) {
        Map<String, Object> source = new LinkedHashMap<>(original.source);
        source.put(name, value);
        return new Doc(original.index, original.id, Collections.unmodifiableMap(source));
    }

    private static List<Doc> core() {
        return List.of(
            document(A, "A", 0, List.of("beta", "alpha", "beta"), "first"),
            document(B, "B", 1, List.of("beta", "alpha"), "second")
        );
    }

    private void withDocs(List<Doc> docs, CheckedRun action) throws Exception {
        assertTrue("bounded retained expansion fixture", docs.size() <= 1024);
        List<String> owned = new ArrayList<>();
        Throwable primary = null;
        try {
            var names = new LinkedHashSet<String>();
            docs.forEach(doc -> names.add(doc.index));
            for (String index : names) {
                assertFalse("fresh owned index required", indexExists(index));
                owned.add(index);
                assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping(mapping()));
            }
            ensureGreen(names.toArray(String[]::new));
            var bulk = client(coordinator).prepareBulk();
            for (Doc doc : docs)
                bulk.add(client(coordinator).prepareIndex(doc.index).setId(doc.id).setSource(doc.source));
            assertFalse(bulk.get().hasFailures());
            refresh(names.toArray(String[]::new));
            action.run();
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            Throwable cleanup = null;
            boolean released = false;
            try {
                assertReleased();
                released = true;
            } catch (Exception | Error failure) {
                cleanup = failure;
            }
            if (released) {
                for (String index : owned) {
                    try {
                        if (indexExists(index)) assertAcked(indicesAdmin().prepareDelete(index));
                    } catch (Exception | Error failure) {
                        cleanup = combine(cleanup, failure);
                    }
                }
                try {
                    assertReleased();
                } catch (Exception | Error failure) {
                    cleanup = combine(cleanup, failure);
                }
            }
            rethrowCleanup(cleanup, primary);
        }
    }

    private static Map<String, Object> mapping() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("@timestamp", Map.of("type", "date_nanos"));
        for (String field : List.of("key", "keys", "actor", "tags", "pad", QUOTED, QUOTED + "_1", "member_1_id")) {
            properties.put(field, Map.of("type", "keyword"));
        }
        properties.put("body", Map.of("properties", Map.of("marker", Map.of("type", "keyword"), "values", Map.of("type", "integer"))));
        return Map.of("properties", properties);
    }

    private static String sequence(String key, int primary, String left, String right) {
        return sequence(key, primary, A, left, B, right);
    }

    private static String sequence(String key, int primary, String first, String left, String last, String right) {
        return "SEQUENCE BY "
            + key
            + " WITH {\"maxspan\":\"1m\"} PRIMARY "
            + primary
            + " (FROM "
            + first
            + " | "
            + left
            + "), (FROM "
            + last
            + " | "
            + right
            + ")";
    }

    private static String retained(String sequence, String pipeline, String sort, int limit) {
        return sequence + " | " + pipeline + " | SORT " + sort + " | LIMIT " + limit + " | RETURN MATCHES";
    }

    private Result query(String query) throws Exception {
        return query(query, 128, false, null, false, false);
    }

    private Result query(String query, int page, boolean columnar) throws Exception {
        return query(query, page, columnar, null, false, false);
    }

    private Result query(String query, int page, boolean columnar, GateMode gateMode, boolean cancel, boolean lowBreaker) throws Exception {
        assertNull("a prior bounded cleanup failure invalidates this test", cleanupViolation);
        int number = LABELS.incrementAndGet();
        assertTrue("bounded private query campaign", number <= 512);
        NodeClient measured = internalCluster().getInstance(NodeClient.class, coordinator);
        ThreadContext context = internalCluster().getInstance(ThreadPool.class, coordinator).getThreadContext();
        // Restore the prior persistent override, including absence; the 128mb node default is not a cluster override.
        String requestLimitKey = HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey();
        String previousPersistentLimit = lowBreaker
            ? clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata().persistentSettings().get(requestLimitKey)
            : null;
        OwnedResponse owned = new OwnedResponse(context);
        active = owned;
        TaskId root = null;
        SearchGate gate = gateMode == null ? null : new SearchGate(gateMode);
        boolean invoked = false, changedBreaker = false;
        Throwable primary = null;
        Result result = null;
        try (var ignored = context.stashContext()) {
            context.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, PREFIX + number);
            if (gate != null) gate.install();
            var request = syncEsqlQueryRequest(query).allowPartialResults(false)
                .profile(true)
                .columnar(columnar)
                .pragmas(new QueryPragmas(Settings.builder().put("page_size", page).put("field_extract_preference", "NONE").build()));
            long deadline = System.nanoTime() + TIMEOUT.nanos();
            invoked = true;
            try {
                var task = measured.executeAndReturnTask(EsqlQueryAction.INSTANCE, request, owned);
                root = new TaskId(measured.getLocalNodeId(), task.getId());
            } catch (TaskCancelledException | IllegalArgumentException | IllegalStateException failure) {
                if (owned.terminal() == false) owned.onFailure(failure);
                throw failure;
            }
            if (gate != null) {
                try {
                    gate.awaitStarted(owned, deadline);
                } catch (Exception | Error failure) {
                    throw new AssertionError("query did not reach the intended real shard gate", failure);
                }
                assertTrue("gate reached with real PIT contexts", contexts() > 0);
                long reservations = 0;
                for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                    reservations += service.getBreaker(CircuitBreaker.REQUEST).getUsed();
                }
                assertTrue("expanded/selected query state must be live", reservations > 0);
                emit(
                    Map.of(
                        "actual_gate",
                        gateMode.name(),
                        "actual_index",
                        gate.index,
                        "actual_fetch_source",
                        gate.payload,
                        "active_contexts",
                        contexts(),
                        "request_reservations",
                        reservations
                    )
                );
                if (lowBreaker) {
                    changedBreaker = true;
                    try {
                        setRequestCircuitBreakerLimit(ByteSizeValue.ofKb(16));
                    } catch (Exception | Error failure) {
                        throw new AssertionError("breaker control failed before native query allocation", failure);
                    }
                }
                if (cancel) {
                    var reply = new PlainActionFuture<Void>();
                    var cancellation = new CancelTasksRequest().setTargetTaskId(root).setReason("retained expansion cancellation proof");
                    cancellation.setWaitForCompletion(false);
                    cancellation.setTimeout(remaining(deadline));
                    try {
                        measured.execute(
                            TransportCancelTasksAction.TYPE,
                            cancellation,
                            ActionListener.wrap(value -> reply.onResponse(null), reply::onFailure)
                        );
                        reply.actionGet(remaining(deadline));
                    } catch (Exception | Error failure) {
                        throw new AssertionError("cancellation control failed before native terminal delivery", failure);
                    }
                }
                gate.release();
            }
            owned.await(remaining(deadline));
            var response = owned.response();
            assertFalse(response.isPartial());
            assertFalse(response.isRunning());
            assertEquals(List.of(), owned.warnings());
            assertNotNull(response.profile());
            List<SequenceSourceStatus> statuses = response.profile()
                .drivers()
                .stream()
                .flatMap(d -> d.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .toList();
            assertEquals("one actual native sequence profile", 1, statuses.size());
            Decoded decoded = query.contains("RETURN MATCHES") ? consumers(response) : null;
            // The decoded retained rows already own their SOURCE maps. Do not keep a second full evidence copy per fanout row.
            List<List<Object>> values = decoded == null
                ? getValuesList(response).stream().map(row -> row.stream().map(SequenceRetainedExpansionIT::heapCell).toList()).toList()
                : decoded.rows.stream().map(SequenceMatchDescription.Row::ordinary).toList();
            int width = decoded == null ? response.columns().size() : decoded.names.size();
            result = new Result(
                decoded,
                values,
                response.columns().subList(0, width).stream().map(ColumnInfo::name).toList(),
                response.columns().subList(0, width).stream().map(ColumnInfo::outputType).toList(),
                statuses.getFirst()
            );
        } catch (Exception | Error failure) {
            primary = failure;
            if (invoked == false && owned.terminal() == false) {
                owned.onFailure(
                    failure instanceof Exception exception
                        ? exception
                        : new IllegalStateException("retained expansion pre-dispatch failure", failure)
                );
            }
            throw failure;
        } finally {
            if (gate != null) gate.release();
            long cleanupDeadline = System.nanoTime() + CLEANUP_TIMEOUT.nanos();
            Throwable cleanup = drain(measured, owned, root, cleanupDeadline);
            if (changedBreaker) {
                try {
                    Settings.Builder restore = Settings.builder();
                    if (previousPersistentLimit == null) restore.putNull(requestLimitKey);
                    else restore.put(requestLimitKey, previousPersistentLimit);
                    assertAcked(
                        clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(restore)
                    );
                } catch (Exception | Error failure) {
                    cleanup = combine(cleanup, failure);
                }
            }
            if (gate != null) {
                try {
                    gate.close(cleanupDeadline);
                } catch (Exception | Error failure) {
                    cleanup = combine(cleanup, failure);
                }
            }
            try {
                assertReleased();
                active = null;
            } catch (Exception | Error failure) {
                cleanup = combine(cleanup, failure);
            }
            if (cleanup != null) cleanupViolation = combine(cleanupViolation, cleanup);
            rethrowCleanup(cleanup, primary);
        }
        emit(
            Map.of(
                "query",
                query,
                "page_size",
                page,
                "columnar",
                columnar,
                "rows",
                result.values.size(),
                "candidate_requests",
                result.status.candidateSearchRequests(),
                "candidate_rows",
                result.status.candidateRowsFetched(),
                "payload_requests",
                result.status.payloadSearchRequests(),
                "stage_drivers",
                result.status.stageDrivers()
            )
        );
        return result;
    }

    private Decoded consumers(EsqlQueryResponse response) throws Exception {
        Decoded decoded = consume(response.responseInternal());
        assertEquals(decoded, fromJson(json(response, false), response.columnar()));
        assertEquals(decoded, fromJson(json(response, true), response.columnar()));
        try (var output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.current());
            response.writeTo(output);
            try (
                var input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), internalCluster().getNamedWriteableRegistry())
            ) {
                input.setTransportVersion(TransportVersion.current());
                try (var copy = EsqlQueryResponse.reader(consumerBlocks).read(input)) {
                    assertEquals(decoded, consume(copy.responseInternal()));
                    assertEquals(decoded, fromJson(json(copy, false), copy.columnar()));
                }
            }
        }
        return decoded;
    }

    private static Decoded consume(EsqlResponse response) {
        List<List<Object>> rows = new ArrayList<>();
        for (var row : response.rows()) {
            List<Object> values = new ArrayList<>();
            row.forEach(value -> values.add(heapCell(value)));
            rows.add(values);
        }
        return decode(response.columns(), rows);
    }

    private static Decoded decode(List<? extends ColumnInfo> columns, List<List<Object>> rows) {
        var description = SequenceMatchDescription.fromColumns(columns);
        List<SequenceMatchDescription.Row> decoded = rows.stream()
            .map(row -> description.decodeRow(row, SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT))
            .toList();
        return new Decoded(description.primaryStage(), description.ordinaryNames(), decoded);
    }

    private static Decoded fromJson(String value, boolean columnar) throws IOException {
        List<ColumnInfo> columns = new ArrayList<>();
        List<List<Object>> values = new ArrayList<>();
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, value)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = parser.currentName();
                parser.nextToken();
                if (name.equals("all_columns")) throw new IllegalArgumentException("column elision outside retained reader profile");
                if (name.equals("columns")) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY)
                        columns.add(ColumnInfoImpl.fromXContent(parser));
                } else if (name.equals("values")) {
                    for (Object item : parser.list()) {
                        if (item instanceof List<?> list) values.add(list.stream().map(SequenceRetainedExpansionIT::heapCell).toList());
                        else throw new IllegalArgumentException("array required");
                    }
                } else parser.skipChildren();
            }
        }
        if (columnar) {
            assertEquals(columns.size(), values.size());
            int height = values.isEmpty() ? 0 : values.getFirst().size();
            List<List<Object>> rows = new ArrayList<>();
            for (int row = 0; row < height; row++) {
                List<Object> cells = new ArrayList<>();
                for (List<Object> column : values) {
                    assertEquals(height, column.size());
                    cells.add(column.get(row));
                }
                rows.add(cells);
            }
            values = rows;
        }
        return decode(columns, values);
    }

    private static Object heapCell(Object value) {
        if (value == null
            || value instanceof String
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Double
            || value instanceof Boolean) return value;
        if (value instanceof List<?> list) return list.stream().map(SequenceRetainedExpansionIT::heapCell).toList();
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, item) -> {
                if (key instanceof String text) copy.put(text, heapCell(item));
                else throw new IllegalArgumentException("SOURCE map string key required");
            });
            return copy;
        }
        throw new IllegalArgumentException("borrowed/unsupported result cell: " + value.getClass().getName());
    }

    private static String json(EsqlQueryResponse response, boolean pretty) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            if (pretty) builder.prettyPrint();
            var params = new ToXContent.MapParams(Map.of("drop_null_columns", "false"));
            var chunks = response.toXContentChunked(params);
            while (chunks.hasNext())
                chunks.next().toXContent(builder, params);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private static void check(Result result, int primary, List<String> names, List<String> types, List<Expected> expected) {
        assertNotNull(result.decoded);
        assertEquals(primary, result.decoded.primary);
        assertEquals(names, result.names);
        assertEquals(types, result.types);
        assertEquals(expected.size(), result.decoded.rows.size());
        for (int i = 0; i < expected.size(); i++) {
            Expected wanted = expected.get(i);
            var actual = result.decoded.rows.get(i);
            assertEquals(wanted.ordinary, actual.ordinary());
            assertEquals(wanted.keys, actual.keys());
            assertEquals(wanted.members.size(), actual.members().size());
            for (int stage = 0; stage < wanted.members.size(); stage++) {
                Doc doc = wanted.members.get(stage);
                var member = actual.members().get(stage);
                assertEquals(stage + 1, member.stage());
                assertEquals(doc.index, member.index());
                assertEquals(doc.id, member.id());
                assertEquals(doc.source, member.source());
            }
        }
    }

    private static Expected expected(Object key, List<Object> ordinary, Doc first, Doc last) {
        return new Expected(cells(key), ordinary, List.of(first, last));
    }

    private static List<Object> cells(Object... values) {
        return Arrays.asList(values);
    }

    private static List<String> keywords(int count) {
        return Collections.nCopies(count, "keyword");
    }

    private static Throwable drain(NodeClient measured, OwnedResponse owned, TaskId root, long deadline) {
        Throwable cleanup = null;
        try {
            owned.close();
        } catch (Exception | Error failure) {
            cleanup = failure;
        }
        if (owned.terminal() == false && root != null) {
            try {
                var reply = new PlainActionFuture<Void>();
                var cancel = new CancelTasksRequest().setTargetTaskId(root).setReason("retained expansion bounded cleanup");
                cancel.setWaitForCompletion(true);
                cancel.setTimeout(remaining(deadline));
                measured.execute(
                    TransportCancelTasksAction.TYPE,
                    cancel,
                    ActionListener.wrap(value -> reply.onResponse(null), reply::onFailure)
                );
                reply.actionGet(remaining(deadline));
            } catch (Exception | Error failure) {
                cleanup = combine(cleanup, failure);
            }
        }
        try {
            if (owned.terminal() == false) owned.await(remaining(deadline));
        } catch (Exception | Error failure) {
            cleanup = combine(cleanup, failure);
        } finally {
            try {
                owned.close();
            } catch (Exception | Error failure) {
                cleanup = combine(cleanup, failure);
            }
        }
        return cleanup;
    }

    private int contexts() {
        int contexts = 0;
        for (SearchService service : internalCluster().getInstances(SearchService.class))
            contexts += service.getActiveContexts();
        return contexts;
    }

    /** Never delete an owned fixture until callbacks, refs, all labelled descendants and native resources are released. */
    @After
    public void assertReleased() throws Exception {
        if (active != null && (active.terminal() == false || active.closed() == false)) {
            throw new IllegalStateException("retained expansion callback must be terminal and owned response closed");
        }
        assertBusy(() -> {
            assertEquals(0, contexts());
            for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                assertEquals(0L, service.getBreaker(CircuitBreaker.REQUEST).getUsed());
            }
            var tasks = client().admin().cluster().prepareListTasks().get().getTasks();
            assertTrue(
                tasks.stream()
                    .noneMatch(task -> task.action().equals(EsqlQueryAction.NAME) || task.action().equals(DriverTaskRunner.ACTION_NAME))
            );
            assertTrue(tasks.stream().noneMatch(task -> {
                String label = task.headers().get(Task.X_OPAQUE_ID_HTTP_HEADER);
                return label != null && label.startsWith(PREFIX);
            }));
        });
        ensureBlocksReleased();
        if (consumerBlocks != null) {
            consumerBlocks.ensureAllBlocksAreReleased();
            assertEquals(0L, consumerBlocks.breaker().getUsed());
        }
        if (cleanupViolation != null) throw new AssertionError(
            "a bounded cleanup failure cannot be accepted as an expected query error",
            cleanupViolation
        );
    }

    private static TimeValue remaining(long deadline) {
        return TimeValue.timeValueNanos(Math.max(1L, deadline - System.nanoTime()));
    }

    private static Throwable combine(Throwable first, Throwable next) {
        if (first == null) return next;
        if (first != next) first.addSuppressed(next);
        return first;
    }

    private static void rethrowCleanup(Throwable cleanup, Throwable primary) throws Exception {
        if (cleanup == null) return;
        if (primary != null) {
            if (primary != cleanup) primary.addSuppressed(cleanup);
        } else if (cleanup instanceof Exception exception) throw exception;
        else if (cleanup instanceof Error error) throw error;
        else throw new AssertionError("unexpected retained expansion cleanup", cleanup);
    }

    private static void emit(Map<String, Object> record) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            String json = Strings.toString(builder.map(record));
            if (json.length() > 65536) throw new IOException("retained expansion observation ceiling");
            System.out.println("RETAINED_EXPANSION_RESULT " + json);
        }
    }

    /** Holds only an independently acquired listener reference; late delivery closes it before terminal completion. */
    private static final class OwnedResponse implements ActionListener<EsqlQueryResponse>, AutoCloseable {
        private final ThreadContext context;
        private final PlainActionFuture<Void> complete = new PlainActionFuture<>();
        private EsqlQueryResponse response;
        private List<String> warnings = List.of();
        private boolean closed;

        private OwnedResponse(ThreadContext context) {
            this.context = context;
        }

        /** Producer callbacks lend their response; the slot needs its own reference before returning. */
        @Override
        public void onResponse(EsqlQueryResponse value) {
            boolean retained = false;
            try {
                value.mustIncRef();
                retained = true;
                synchronized (this) {
                    warnings = List.copyOf(context.getResponseHeaders().getOrDefault("Warning", List.of()));
                    if (closed == false) {
                        response = value;
                        retained = false;
                    }
                }
                if (retained) {
                    retained = false;
                    value.close();
                }
                complete.onResponse(null);
            } catch (Exception | Error failure) {
                if (retained) {
                    try {
                        value.close();
                    } catch (Exception | Error closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                complete.onFailure(
                    failure instanceof Exception exception
                        ? exception
                        : new IllegalStateException("retained expansion callback failed", failure)
                );
            }
        }

        /** Native failure ends the wait without manufacturing a retained response. */
        @Override
        public void onFailure(Exception failure) {
            complete.onFailure(failure);
        }

        private void await(TimeValue timeout) {
            complete.actionGet(timeout);
        }

        private boolean terminal() {
            return complete.isDone();
        }

        private synchronized boolean closed() {
            return closed;
        }

        private synchronized EsqlQueryResponse response() {
            assertNotNull(response);
            return response;
        }

        private synchronized List<String> warnings() {
            return warnings;
        }

        /** Atomic detach keeps timeout/late-delivery cleanup from closing the same owned reference twice. */
        @Override
        public void close() {
            EsqlQueryResponse release;
            synchronized (this) {
                closed = true;
                release = response;
                response = null;
            }
            if (release != null) release.close();
        }
    }

    /** Targets existing native shard phases without a production service or wire hook. */
    private enum GateMode {
        CANDIDATE,
        PAYLOAD
    }

    /** Reuses the real shard gate pattern, retaining its request while parked on the existing generic pool. */
    private final class SearchGate {
        private final GateMode mode;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final CountDownLatch started = new CountDownLatch(1), proceed = new CountDownLatch(1), handled = new CountDownLatch(1);
        private volatile boolean closed;
        private String index;
        private boolean payload;

        private SearchGate(GateMode mode) {
            this.mode = mode;
        }

        private void install() {
            for (String node : internalCluster().getNodeNames()) {
                var transport = MockTransportService.getInstance(node);
                transport.<ShardSearchRequest>addRequestHandlingBehavior(
                    SearchTransportService.QUERY_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        boolean fetchSource = request.source() != null
                            && request.source().fetchSource() != null
                            && request.source().fetchSource().fetchSource();
                        if (closed
                            || request.shardId().getIndexName().equals(B) == false
                            || fetchSource != (mode == GateMode.PAYLOAD)
                            || claimed.compareAndSet(false, true) == false) {
                            handler.messageReceived(request, channel, task);
                            return;
                        }
                        index = request.shardId().getIndexName();
                        payload = fetchSource;
                        request.incRef();
                        try {
                            transport.getThreadPool().generic().execute(new AbstractRunnable() {
                                @Override
                                protected void doRun() throws Exception {
                                    started.countDown();
                                    if (proceed.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS) == false) {
                                        throw new IllegalStateException("retained expansion gate not released");
                                    }
                                    handler.messageReceived(request, channel, task);
                                }

                                @Override
                                public void onFailure(Exception failure) {
                                    if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                                    channel.sendResponse(failure);
                                }

                                @Override
                                public void onAfter() {
                                    request.decRef();
                                    handled.countDown();
                                }
                            });
                        } catch (Exception | Error failure) {
                            request.decRef();
                            handled.countDown();
                            throw failure;
                        }
                    }
                );
            }
        }

        private void awaitStarted(OwnedResponse query, long deadline) throws Exception {
            while (started.await(50, TimeUnit.MILLISECONDS) == false) {
                if (query.terminal()) {
                    query.await(remaining(deadline));
                    fail("query finished before its gate");
                }
                assertTrue("retained expansion gate deadline", System.nanoTime() < deadline);
            }
        }

        private void release() {
            proceed.countDown();
        }

        private void close(long deadline) throws Exception {
            closed = true;
            release();
            for (String node : internalCluster().getNodeNames())
                MockTransportService.getInstance(node).clearAllRules();
            if (claimed.get()) assertTrue(
                "gate request must finish ownership transfer",
                handled.await(Math.max(1L, remaining(deadline).millis()), TimeUnit.MILLISECONDS)
            );
        }
    }

    /** Inert stored-source fixture and physical identity. */
    private record Doc(String index, String id, Map<String, Object> source) {}

    /** Exact processed cells, scalar keys and row-local genuine members. */
    private record Expected(List<Object> keys, List<Object> ordinary, List<Doc> members) {}

    /** Reference-reader output, with no global physical-event deduplication. */
    private record Decoded(int primary, List<String> names, List<SequenceMatchDescription.Row> rows) {}

    /** Heap-owned consumer result and actual immutable native aggregate status. */
    private record Result(
        Decoded decoded,
        List<List<Object>> values,
        List<String> names,
        List<String> types,
        SequenceSourceStatus status
    ) {}

    /** Runs one bounded fixture action before owned cleanup. */
    @FunctionalInterface
    private interface CheckedRun {
        /** Keeps fixture-owned work inside the enclosing liveness and index-ownership cleanup. */
        void run() throws Exception;
    }
}
