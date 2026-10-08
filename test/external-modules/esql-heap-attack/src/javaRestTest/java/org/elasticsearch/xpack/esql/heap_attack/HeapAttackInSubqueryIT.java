/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that run ESQL queries with {@code WHERE field IN (subquery)} predicates that use a ton of memory. We want to make sure they don't
 * consume the entire heap and crash Elasticsearch.
 * <p>
 * Four shapes are exercised:
 * <ul>
 *     <li>The size of the intermediate results LocalRelation exceeds esql.intermediate_local_relation_max_size.</li>
 *     <li>The size of the intermediate results trips the BlockHash used by duplication removal in AbstractSubqueryJoin. This shared
 *     dedup path is exercised for all three join shapes: IN (SemiJoin), NOT IN (AntiJoin) and IN-under-OR (MarkJoin).</li>
 *     <li>The IN subquery carries a large amount of data (many big fields, or a giant text field) through to the output.</li>
 *     <li>The IN subquery carries a sort over many big fields or a grouping aggregation with a large grouping set.</li>
 * </ul>
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackInSubqueryIT extends HeapAttackTestCase {

    // Reuse HeapAttackIT methods to prepare the indices.
    private static final HeapAttackIT heapAttackIT = new HeapAttackIT();

    // Stay above esql.in_subquery_hash_join_threshold (100) so the IN subquery runs as a hash join
    private static final int MAX_DOC = 350;

    private static final int STRING_FIELD_1000 = 1000;

    @Before
    public void checkCapability() {
        assumeTrue("Run these tests in snapshot build", Build.current().isSnapshot());
    }

    /**
     * Trips the {@code esql.intermediate_local_relation_max_size} guard rather than the circuit breaker: the IN subquery returns more data
     * (~32MB) than the cap (default {@code min(2% heap, 100mb)} ≈ 10mb on the heap-attack cluster), so {@code EsqlSession.resultToPlan}
     * rejects the subquery result with a {@code 400 illegal_argument_exception}.
     */
    public void testIntermediateLocalRelationMaxSizeWithInSubquery() throws IOException {
        heapAttackIT.initGiantTextField(20, true, 1, true);
        try {
            Map<String, Object> response = giantTextInSubquery();
            fail("expected the intermediate local relation size guard to reject the subquery result but got " + response);
        } catch (ResponseException e) {
            assertMap(
                responseAsMap(e.getResponse()),
                matchesMap().extraOk()
                    .entry("status", 400)
                    .entry(
                        "error",
                        matchesMap().extraOk()
                            .entry("type", "illegal_argument_exception")
                            .entry("reason", containsString("sub-plan execution results too large"))
                    )
            );
        }
    }

    /**
     * Trips the circuit breaker inside the IN-subquery <em>duplication removal</em> step, where {@code AbstractSubqueryJoin} runs the
     * subquery's key column through a {@link org.elasticsearch.compute.aggregation.blockhash.BlockHash} on the coordinator before deciding
     * the join shape. The values are distinct giant texts so the dedup {@code BlockHash} retains one entry per document, and the
     * per-request {@code esql.intermediate_local_relation_max_size} cap is raised so the large subquery result reaches dedup instead
     * of being rejected by {@code checkPagesBelowSize} first.
     * <p>
     * The subquery shape is randomized per attempt by {@link #giantTextInSubquery(int)}: {@code IN} (a {@code SemiJoin}), {@code NOT IN}
     * (an {@link org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin AntiJoin}), or {@code IN (...) OR id < 0} (a
     * {@link org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin MarkJoin}). All three are siblings of {@code AbstractSubqueryJoin}
     * and run the subquery key column through the <em>same</em> dedup {@code BlockHash} in {@code inlineData} before any
     * join-type-specific routing, so each trips the breaker identically — the memory math below is independent of which shape is picked.
     * <p>
     * The dedup {@code BlockHash} allocates through the coordinator's {@code BlockFactory}, which is built on {@code bigArrays} so it
     * charges the <em>request</em> breaker, which is capped at 60% of heap {@code -Xmx512m}, that is about {@code 307.1mb}.
     * <p>
     * Each of the 64 indexed documents carries a <em>distinct</em> ~1mb text value, so the dedup {@code BlockHash} cannot collapse any
     * rows: it keeps one full ~1mb entry per subquery row. At the peak of dedup (inside {@code BlockHash#getKeys}) four full-size copies
     * of that key column are alive on the request breaker at once (measured at {@code LIMIT 52}, see below):
     * <ul>
     *   <li>the raw subquery-result page ({@code result.pages()}, the {@code checkPagesBelowSize} input) held by the caller,
     *   ~1× ≈ 52mb;</li>
     *   <li>the {@code LocalRelation} key column that dedup reads — the {@code SessionUtils.fromPages} rebuild that dedup runs through the
     *   hash — charged at ×2.5 ≈ 130mb;</li>
     *   <li>the {@code BytesRefHash}'s own retained copy of every distinct value, ~1× ≈ 52mb (it uses {@code BigArrays} directly, not a
     *   {@code BytesRef} block, so the ×2.5 below does not apply);</li>
     *   <li>the freshly allocated {@code getKeys} output block, charged at ×2.5 ≈ 130mb.</li>
     * </ul>
     * Note this column is single-valued, so {@code mayHaveMultivaluedFields()} is {@code false} and {@code convertMvPositionsToNull} makes
     * <em>no</em> extra MV-normalized copy here.
     * <p>
     * The ×2.5 on two of those four copies is the {@code BytesRef} RAM overestimate: ES|QL multiplies the RAM estimate of a
     * {@code BytesRef} block by {@code esql.bytes_ref_ram_overestimate_factor} (2.5) when the block's <em>average RAM per value</em>
     * exceeds {@code esql.bytes_ref_ram_overestimate_threshold} (1mb), to cover untracked {@code _source}/page overhead. The trigger is
     * {@code RamUsageEstimator.sizeOf(bytesRefArray) / valueCount > 1mb} (see {@code BytesRefArrayVector#ramBytesEstimated}), i.e. it
     * compares RAM-per-value, <em>not</em> the raw payload length. So even though each text payload is exactly 1mb (1048576 bytes), the
     * per-value RAM (~1049092 bytes once the {@code startOffsets} long and array/page overhead are spread across the values) sits just
     * above the 1mb threshold and flips the comparison — which is why the factor really does apply to these blocks.
     * <p>
     * Measured (single traced run, {@code LIMIT 52}, attempt 1): the {@code LocalRelation} key column dedup reads is
     * {@code ramBytesUsed=136382028} for 52 values ({@code 2622731} bytes/value ≈ ×2.5 of the ~1048648-byte raw value), confirming the
     * factor is applied. The request breaker is at {@code 190911724} entering dedup (that key column plus the held ~52mb raw result
     * page), climbs to {@code 245486164} just before {@code getKeys} (the {@code BytesRefHash} added its own ~52mb 1× copy), then the
     * {@code getKeys} output reservation (another {@code 136382028}) pushes it over:
     * {@code Data too large, data for [<esql_block_factory>] would be [381868192/364.1mb], which is larger than the limit of
     * [322122547/307.1mb]}. The {@code CircuitBreakingException} fires <em>inside</em> that {@code getKeys} allocation, so its stack
     * contains both {@code AbstractSubqueryJoin} (the dedup caller) and {@code BlockHash} (the allocation site), which is what
     * {@link #assertCircuitBreaksVia} matches on.
     * <p>
     * The workload is still ramped — {@code giantTextInSubquery(48 + attempt * 4)} sets {@code LIMIT} to {@code 52, 56, 60, 64} on
     * attempts 1-4 (attempt 5 would ask for 68 but only 64 docs exist, so it clamps to 64) — so the test stays robust even though
     * attempt 1 suffices in practice: {@link #assertCircuitBreaksVia} keeps retrying with a larger subquery if a run does not cross the
     * ceiling on the first try (the exact charge drifts run-to-run) or if an upstream operator trips first via a different stack.
     * <p>
     * The two tuning constants keep dedup as the thing that breaks, and the ×2.5 is the reason the {@code 128mb} cap is <em>not</em> hit
     * first even though the breaker later climbs to 364mb. {@code checkPagesBelowSize} runs against the <em>raw</em> {@code result.pages()}
     * — and crucially those blocks are <em>not</em> the {@code BytesRefArray}-backed representation the overestimate targets, so they are
     * charged at ~1×. The giant ~1mb values come back as one page per row (measured: 52 single-value {@code BytesRefVectorBlock}s, each
     * {@code ramBytesUsed=1048648}), so the cap sees {@code capSize=54529696} (~52mb at {@code LIMIT 52}, ~64mb at {@code LIMIT 64}) — well
     * under {@code 128mb}. The ×2.5 only appears once {@code SessionUtils.fromPages} re-materializes those rows into a single
     * {@code BytesRefArray}-backed block (the {@code 136382028}-byte dedup input) and again at {@code getKeys}. So the cap is bigger than
     * the result it measures (~52mb), while the breaker breaks on the inflated dedup copies it never sees — which is exactly why the
     * result reaches dedup instead of being rejected first (the failure mode exercised by
     * {@link #testIntermediateLocalRelationMaxSizeWithInSubquery()}).
     */
    public void testDedupBlockHashWithInSubquery() throws IOException {
        // 64 docs, each a distinct ~1mb text field, so the dedup BlockHash retains one ~1mb entry per row and cannot collapse anything.
        heapAttackIT.initGiantTextField(64, true, 1, true);
        // Raise the per-request intermediate-result cap (default min(2% heap, 100mb) ≈ 10mb here) above the ~64mb result so it reaches
        // dedup rather than being rejected by checkPagesBelowSize. 128mb stays above the result's estimated size across every ramp step.
        setIntermediateLocalRelationMaxSize("128mb");
        try {
            // Ramp the subquery LIMIT (52, 56, 60, 64) so the coexisting dedup copies grow until the request breaker's 307.1mb ceiling
            // (60% of the 512mb heap) is crossed inside BlockHash. In practice attempt 1 (LIMIT 52) already reaches ~364mb and trips.
            assertCircuitBreaksVia(attempt -> giantTextInSubquery(48 + attempt * 4), "AbstractSubqueryJoin", "BlockHash");
        } finally {
            setIntermediateLocalRelationMaxSize(null);
        }
    }

    /**
     * Trips the circuit breaker in the <em>main</em> query's wide source scan, <em>not</em> inside the IN subquery. The subquery here is
     * a trivial projection ({@code FROM manybigfields | KEEP f000}) that carries a single ~1kb key column for {@link #MAX_DOC} rows, so
     * it finishes well under any limit. Once it resolves into the IN filter, the outer {@code FROM manybigfields | WHERE f000 IN (...)}
     * runs as a {@code SELECT *} over all {@link #STRING_FIELD_1000} fields, and materializing those wide rows is what breaks.
     * <p>
     * Where/when it breaks:
     * <ul>
     *   <li><b>Query phase:</b> the <em>main</em> query, after the subquery has already become the IN filter.</li>
     *   <li><b>Operator:</b> {@code ValuesSourceReaderOperator}, while it materializes the row into {@code BytesRefBlock}s; on serverless
     *   it could also happen during exchange when sending data between nodes.</li>
     * </ul>
     */
    public void testRandomKeywordOrTextFieldsWithInSubquery() throws IOException {
        String dataType = keywordOrText();
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, dataType, true, STRING_FIELD_1000);
        assertCircuitBreaks(attempt -> inSubqueryKeepManyFields());
    }

    /**
     * Similar query phase and operator as {@link #testRandomKeywordOrTextFieldsWithInSubquery()}: the <em>request</em> breaker
     * ({@code 307.1mb}, 60% of the 512mb heap) trips inside the <em>main</em> query.
     * <p>
     * What this test adds over the plain projection is the <em>subquery</em> shape: {@code SORT f000..f099 | KEEP f000 | LIMIT MAX_DOC}
     * runs a {@code TopNOperator} over a 100-column big-string sort key (local sort on the data node, merge on the coordinator). That
     * sort only needs the 100 sort-key fields (~100kb/row × {@link #MAX_DOC} rows ≈ 35mb) and stays under the breaker, so the subquery
     * <em>succeeds</em>.
     */
    public void testSortWithInSubquery() throws IOException {
        String dataType = keywordOrText();
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, dataType, true, STRING_FIELD_1000);
        assertCircuitBreaks(attempt -> inSubqueryWithSort(sortOrGroupingColumns(100), true));
    }

    /**
     * Similar query phase and operator as {@link #testRandomKeywordOrTextFieldsWithInSubquery()}: the <em>request</em> breaker
     * ({@code 307.1mb}) trips on a <em>data</em> node inside the <em>main</em> query.
     * <p>
     * The distinctive part is the <em>subquery</em> aggregation: {@code STATS COUNT_DISTINCT(f499) BY f000..f099 | KEEP f000} runs a
     * grouping {@code HashAggregationOperator}/{@code BlockHash} over a 100-column composite key (partial aggregation on the data node,
     * final on the coordinator). Because the fields are random, every row is its own group (~{@link #MAX_DOC} groups), so the grouping
     * state holds ~{@link #MAX_DOC} × 100 × ~1kb ≈ 35mb of key data plus the {@code COUNT_DISTINCT} HLL sketches — under the breaker. So
     * the subquery <em>succeeds</em>.
     */
    public void testAggWithInSubquery() throws IOException {
        String dataType = keywordOrText();
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, dataType, true, STRING_FIELD_1000);
        assertCircuitBreaks(attempt -> inSubqueryWithAgg("c = COUNT_DISTINCT(f499)", sortOrGroupingColumns(100)));
    }

    /**
     * Unlike {@link #testRandomKeywordOrTextFieldsWithInSubquery()}, {@link #testSortWithInSubquery()} and
     * {@link #testAggWithInSubquery()} — which all let the subquery succeed and break later in the main query's wide scan — this test
     * forces the breaker to trip <em>inside the {@code NOT IN} subquery</em>, before its result ever reaches the coordinator dedup or the
     * main query.
     * <p>
     * The subquery sorts by <em>all</em> {@link #STRING_FIELD_1000} fields ({@code SORT f000..f999}), so its {@code TopNOperator} must
     * load and hold every field for all {@link #MAX_DOC} rows: ~{@link #STRING_FIELD_1000} × ~1kb × {@link #MAX_DOC} ≈ 350mb, over the
     * {@code 307.1mb} request breaker (60% of the 512mb heap). The IN subquery is executed as an independent sub-plan <em>before</em> the
     * main query, so this sort trips on a data node during subquery execution: the subquery never produces a result and the main query
     * never runs. The {@code NOT IN} would resolve to an {@link org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin AntiJoin}, but it
     * never gets that far: the subquery fails first.
     * <p>
     * Measured (single traced run): the breaker trips on a data node with the data label {@code [topn]} at {@code 308.2mb} inside
     * {@code TopNOperator.addInput}, while {@code Utf8AscTopNEncoder} encodes the wide big-string sort key — confirming the failure is the
     * subquery's sort, not the main query. There is exactly one {@code SORT} in the query (the subquery's), so this is a robust signal.
     */
    public void testNotInSubqueryBreaksInSubquery() throws IOException {
        String dataType = keywordOrText();
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, dataType, true, STRING_FIELD_1000);
        assertCircuitBreaks(attempt -> inSubqueryWithSort(sortOrGroupingColumns(STRING_FIELD_1000), false));
    }

    private Map<String, Object> inSubqueryKeepManyFields() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | WHERE f000 IN (FROM manybigfields | KEEP f000)");
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> giantTextInSubquery() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM bigtext | WHERE f IN (FROM bigtext | KEEP f)");
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    /**
     * Builds the ramped IN-subquery query for {@link #testDedupBlockHashWithInSubquery()}, randomly choosing one of the three join shapes
     * that {@code InSubqueryResolver} produces and that all share {@code AbstractSubqueryJoin}'s coordinator-side dedup {@code BlockHash}:
     * <ul>
     *   <li>{@code 0}: {@code f IN (subquery)} &rarr; {@code SemiJoin};</li>
     *   <li>{@code 1}: {@code f NOT IN (subquery)} &rarr; {@link org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin AntiJoin};</li>
     *   <li>{@code 2}: {@code f IN (subquery) OR id < 0} &rarr; {@link org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin MarkJoin}
     *   (the {@code IN} sits under an {@code OR}; {@code id < 0} never matches since ids are {@code 0..63}, so it only forces the MarkJoin
     *   shape without changing the result).</li>
     * </ul>
     * All three run the subquery key column through the same dedup {@code BlockHash} before any join-type-specific routing, so each trips
     * the request breaker the same way; see {@link #testDedupBlockHashWithInSubquery()} for the memory math.
     */
    private Map<String, Object> giantTextInSubquery(int subqueryDocs) throws IOException {
        int joinShape = randomIntBetween(0, 2);
        StringBuilder query = startQuery();
        query.append("FROM bigtext | WHERE f ");
        if (joinShape == 1) {
            query.append("NOT ");
        }
        query.append("IN (FROM bigtext | KEEP f | LIMIT ").append(subqueryDocs).append(")");
        if (joinShape == 2) {
            query.append(" OR id < 0");
        }
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> inSubqueryWithSort(String sortKeys, boolean in) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | WHERE f000 ")
            .append(in ? "IN" : "NOT IN")
            .append(" (FROM manybigfields | SORT ")
            .append(sortKeys)
            .append(" | KEEP f000 | LIMIT ")
            .append(MAX_DOC)
            .append(")");
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> inSubqueryWithAgg(String agg, String grouping) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | WHERE f000 IN (FROM manybigfields | STATS ").append(agg);
        if (grouping != null && grouping.isEmpty() == false) {
            query.append(" BY ").append(grouping);
        }
        query.append(" | KEEP f000)");
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private static String sortOrGroupingColumns(int columnCount) {
        StringBuilder keys = new StringBuilder(fieldName(0));
        for (int f = 1; f < columnCount; f++) {
            keys.append(", ").append(fieldName(f));
        }
        return keys.toString();
    }

    private static String fieldName(int f) {
        return "f" + String.format(Locale.ROOT, "%03d", f);
    }

    private static String keywordOrText() {
        return randomBoolean() ? "keyword" : "text";
    }

    /**
     * Change (or resets, when {@code size} is {@code null}) the maximum size of a subplan(subquery)'s intermediate {@code LocalRelation}.
     * The default is {@code min(2% heap, 100mb)}, which on the small heap-attack cluster rejects large subquery results before they reach
     * the IN-subquery dedup {@code BlockHash}. Tests that want to exercise the dedup breaker should raise it first.
     */
    protected void setIntermediateLocalRelationMaxSize(String size) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{\"persistent\": {\"esql.intermediate_local_relation_max_size\": " + (size == null ? "null" : "\"" + size + "\"") + "}}"
        );
        adminClient().performRequest(request);
    }
}
