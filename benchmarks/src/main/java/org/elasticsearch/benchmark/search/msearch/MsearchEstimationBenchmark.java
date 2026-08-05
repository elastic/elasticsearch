/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.msearch;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.suggest.SortBy;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.xcontent.Text;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures the per-call overhead of {@link TransportMultiSearchAction#estimateActualBytes} across
 * the estimation paths identified in review. Each {@code responseShape} exercises a
 * distinct code branch:
 * <ul>
 *   <li><b>base</b> — hits with raw source bytes and two document fields; no optional components.
 *       Exercises the field-value structural walk in {@code estimateHitBytes}.</li>
 *   <li><b>highlights</b> — base + one highlight fragment per hit. Triggers
 *       {@code CountingStreamOutput} serialization once per hit.</li>
 *   <li><b>explanations</b> — base + one {@link Explanation} per hit. Triggers
 *       {@code CountingStreamOutput} serialization once per hit via {@code writeExplanation}.</li>
 *   <li><b>sortValues</b> — base + two sort values (Long + String) per hit with
 *       {@link DocValueFormat#RAW}. Exercises the formatted and raw sort-value walk, including
 *       the per-value {@code estimateValueBytes} calls for both arrays.</li>
 *   <li><b>innerHits</b> — base + one inner hit per outer hit (1 level). Exercises the first
 *       level of recursive {@code estimateHitBytes} calls for nested documents.</li>
 *   <li><b>deepInnerHits</b> — base + 3-level nesting (outer → inner → deepest). Exercises
 *       the full recursion depth for deeply nested documents.</li>
 *   <li><b>suggest</b> — base + a {@link Suggest} blob with three {@link TermSuggestion} entries
 *       and one option each. Triggers one {@code CountingStreamOutput} call for the entire
 *       response-level suggest object.</li>
 *   <li><b>profile</b> — base + a {@link SearchProfileResults} with two shards, each containing
 *       a query phase result (one {@code TermQuery} with collector) and a fetch phase result.
 *       Triggers one {@code CountingStreamOutput} call for the entire profile object.</li>
 *   <li><b>shardFailures</b> — base + three {@link ShardSearchFailure}s. Exercises the
 *       {@code estimateExceptionBytes} structural walk (cause chain + stack frames + message).</li>
 *   <li><b>clusters</b> — base + a CCS {@link SearchResponse.Clusters} with two remote cluster
 *       entries. {@link SearchResponse.Clusters#hasClusterObjects()} must be true to trigger the
 *       {@code CountingStreamOutput} call; constructors that take only totals (int, int, int) always
 *       produce an empty {@code clusterInfo} map and would silently skip the serialization.</li>
 *   <li><b>full</b> — all of the above combined. Because {@code innerHits} and {@code deepInnerHits}
 *       are both active, each outer hit ends up with two inner-hit map entries ({@code "nested_docs"}
 *       from {@code innerHits} and {@code "level1"} from {@code deepInnerHits}).</li>
 * </ul>
 * <p>
 * The aggregation path ({@code DelayableWriteable.getUncompressedSerializedSize}) is intentionally
 * excluded: it is only reached when {@code hasAggregations()} is true and
 * {@code getQueryPhaseAggregationBreakerBytes()} is zero, which does not occur in the normal msearch
 * flow where aggregation bytes are transferred via handoff. Aggregation serialization overhead is
 * covered by the existing {@code StringTermsSerializationBenchmark}.
 * <p>
 * Run with:
 * <pre>
 *   ./gradlew :benchmarks:run --args="MsearchEstimationBenchmark"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Thread)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class MsearchEstimationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Number of hits in the sub-search response. */
    @Param({ "10", "100", "1000" })
    public int hitCount;

    /** Which optional response components to include — see class Javadoc for details. */
    @Param(
        {
            "base",
            "highlights",
            "explanations",
            "sortValues",
            "innerHits",
            "deepInnerHits",
            "suggest",
            "profile",
            "shardFailures",
            "clusters",
            "full" }
    )
    public String responseShape;

    public SearchResponse response;

    @Setup
    public void setup() {
        boolean doHighlights = responseShape.equals("highlights") || responseShape.equals("full");
        boolean doExplanations = responseShape.equals("explanations") || responseShape.equals("full");
        boolean doSortValues = responseShape.equals("sortValues") || responseShape.equals("full");
        boolean doInnerHits = responseShape.equals("innerHits") || responseShape.equals("full");
        boolean doDeepInnerHits = responseShape.equals("deepInnerHits") || responseShape.equals("full");
        boolean doSuggest = responseShape.equals("suggest") || responseShape.equals("full");
        boolean doProfile = responseShape.equals("profile") || responseShape.equals("full");
        boolean doFailures = responseShape.equals("shardFailures") || responseShape.equals("full");
        boolean doClusters = responseShape.equals("clusters") || responseShape.equals("full");

        // Shared sort format array — DocValueFormat.RAW is stateless, safe to reuse across hits.
        DocValueFormat[] sortFormats = new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW };

        byte[] source = new byte[500];
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            SearchHit hit = new SearchHit(i, "id-" + i);
            hit.sourceRef(new BytesArray(source));
            hit.addDocumentFields(
                Map.of(
                    "keyword_field",
                    new DocumentField("keyword_field", List.of("some-keyword-value")),
                    "long_field",
                    new DocumentField("long_field", List.of(42L))
                ),
                Map.of()
            );
            if (doHighlights) {
                hit.highlightFields(
                    Map.of(
                        "body",
                        new HighlightField("body", new Text[] { new Text("this is a <em>highlighted</em> fragment in the body field") })
                    )
                );
            }
            if (doExplanations) {
                // vary by index so each hit gets a distinct Explanation, preventing JIT constant-folding of the serialization
                hit.explanation(Explanation.match(1.5f, "weight(body:elasticsearch in " + i + ") [PerFieldSimilarity]"));
            }
            if (doSortValues) {
                // Two sort fields: a Long (numeric, common in date/numeric sorts) and a String keyword.
                // Vary by index for the same constant-folding reason as explanations above.
                hit.sortValues(new Object[] { (long) i, "keyword-" + i }, sortFormats);
            }
            if (doInnerHits) {
                SearchHit innerHit = new SearchHit(0, "inner-" + i);
                innerHit.sourceRef(new BytesArray(new byte[200]));
                hit.setInnerHits(
                    Map.of("nested_docs", new SearchHits(new SearchHit[] { innerHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f))
                );
            }
            if (doDeepInnerHits) {
                SearchHit deepest = new SearchHit(0, "deepest-" + i);
                deepest.sourceRef(new BytesArray(new byte[100]));
                SearchHit inner = new SearchHit(0, "inner-" + i);
                inner.sourceRef(new BytesArray(new byte[200]));
                inner.setInnerHits(
                    Map.of("level2", new SearchHits(new SearchHit[] { deepest }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f))
                );
                hit.setInnerHits(
                    Map.of("level1", new SearchHits(new SearchHit[] { inner }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f))
                );
            }
            hits[i] = hit;
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO), 1f);

        Suggest suggest = null;
        if (doSuggest) {
            // Three misspelled terms with one correction option each — a representative suggest payload.
            TermSuggestion termSuggestion = new TermSuggestion("did-you-mean", 5, SortBy.SCORE);
            for (String[] pair : new String[][] { { "elasticserach", "elasticsearch" }, { "serach", "search" }, { "qurey", "query" } }) {
                TermSuggestion.Entry entry = new TermSuggestion.Entry(new Text(pair[0]), 0, pair[0].length());
                entry.addOption(new TermSuggestion.Entry.Option(new Text(pair[1]), 100, 0.9f));
                termSuggestion.addTerm(entry);
            }
            suggest = new Suggest(new ArrayList<>(List.of(termSuggestion)));
        }

        SearchProfileResults profileResults = null;
        if (doProfile) {
            // Realistic two-shard profile: one TermQuery with a collector + fetch phase per shard.
            ProfileResult queryResult = new ProfileResult(
                "TermQuery",
                "body:elasticsearch",
                Map.of("score", 12345L, "next_doc", 6789L, "compute_max_score", 2000L),
                Map.of(),
                21134L,
                List.of()
            );
            CollectorResult collector = new CollectorResult("SimpleTopScoreDocCollector", "search top docs", 5000L, List.of());
            QueryProfileShardResult queryShardResult = new QueryProfileShardResult(List.of(queryResult), 1000L, collector, null);
            SearchProfileQueryPhaseResult queryPhase = new SearchProfileQueryPhaseResult(
                List.of(queryShardResult),
                new AggregationProfileShardResult(List.of())
            );
            ProfileResult fetchPhaseResult = new ProfileResult(
                "FetchPhase",
                "fetch [1 fields]",
                Map.of("next_reader", 100L, "load_source", 2000L, "load_stored_fields", 500L),
                Map.of(),
                2600L,
                List.of()
            );
            SearchProfileShardResult shardResult = new SearchProfileShardResult(queryPhase, fetchPhaseResult);
            Map<String, SearchProfileShardResult> shards = new HashMap<>();
            shards.put("[node1][my-index][0]", shardResult);
            shards.put("[node2][my-index][1]", shardResult);
            profileResults = new SearchProfileResults(shards);
        }

        ShardSearchFailure[] shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        if (doFailures) {
            // Three failures share one RuntimeException instance. estimateExceptionBytes walks the
            // cause chain independently for each failure, so it charges the exception cost three times —
            // an intentional over-estimate in production code. The benchmark intentionally mirrors that.
            RuntimeException cause = new RuntimeException("simulated shard failure: index out of range in shard [index][0]");
            shardFailures = new ShardSearchFailure[] {
                new ShardSearchFailure(cause),
                new ShardSearchFailure(cause),
                new ShardSearchFailure(cause) };
        }

        SearchResponse.Clusters clusters;
        if (doClusters) {
            // Use the Map-based constructor: (int, int, int) always sets clusterInfo=emptyMap(), so
            // hasClusterObjects() would be false and the serialization branch would never fire.
            Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
            clusterMap.put("remote1", new SearchResponse.Cluster("remote1", "remote1:logs-*", false, null));
            clusterMap.put("remote2", new SearchResponse.Cluster("remote2", "remote2:logs-*", true, null));
            clusters = new SearchResponse.Clusters(clusterMap);
        } else {
            clusters = SearchResponse.Clusters.EMPTY;
        }

        response = new SearchResponse(
            searchHits,
            InternalAggregations.EMPTY,
            suggest,
            false,
            null,
            profileResults,
            1,
            null,
            5,
            5,
            0,
            10L,
            shardFailures,
            clusters
        );
        // SearchResponse.constructor incRefs searchHits; release our own ref so the response is the sole owner.
        // When response.decRef() reaches zero it will call searchHits.decRef(), cascading into each SearchHit.
        searchHits.decRef();
    }

    @TearDown
    public void tearDown() {
        response.decRef();
    }

    @Benchmark
    public void estimate(Blackhole bh) {
        bh.consume(TransportMultiSearchAction.estimateActualBytes(response));
    }
}
