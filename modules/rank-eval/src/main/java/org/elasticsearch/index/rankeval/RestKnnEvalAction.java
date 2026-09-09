/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Estimates ANN recall for a {@code dense_vector} field by comparing cheap search configurations against a more thorough one.
 * <pre>
 * POST /my-index/_knn_eval
 * {
 *   "field": "emb",
 *   "k": 10,
 *   "sample": { "size": 100, "seed": 42 },
 *   "baseline":   { "exact": true },
 *   "candidates": [ { "visit_percentage": 2 }, { "visit_percentage": 5 } ],
 *   "include_details": false,
 *   "filter": { "term": { "tenant": "acme" } },
 *   "max_queries_per_batch": 50,
 *   "max_concurrent_searches": 1,
 *   "include_fidelity": false
 * }
 * </pre>
 * A knobs object takes {@code visit_percentage}, {@code num_candidates} and {@code oversample}, and a baseline may instead set
 * {@code exact: true} (on its own, and only a baseline). <b>An omitted baseline defaults to {@code {"exact": true}}</b>, so the reported
 * recall is true recall unless a caller asks for something cheaper. For a large sweep, certify a cheap baseline against an exact one on
 * a small sample first and then pass {@code baseline: {visit_percentage: 100, oversample: N}}.
 * <p>
 * The exact baseline brute-forces every document with a vector: true ground truth rather than the 100%-visit proxy. It requires
 * {@code search.allow_expensive_queries}, which a non-exact baseline never does, being an ordinary kNN search. It costs
 * O(N x dims x 4 bytes) per query -- roughly 95 ms per query over 500k 1024-dimensional vectors already in the page cache, seconds per
 * query once 10M vectors come off disk -- so pair it with a small {@code sample.size}.
 * <p>
 * {@code baseline_vector_ops_kind} says which unit {@code baseline_vector_ops} is in. An exact baseline reports
 * {@code full_precision_scan}: one float32 comparison per live vector, taken from the scan's own hit count since the profiler records
 * {@code vector_operations_count} only for the approximate path. An approximate baseline reports
 * {@code quantized_visit_plus_rescore}: mostly 1-bit comparisons over the visited posting lists, plus the full-precision rescore
 * window. The two are comparable as "work done", not as bytes touched.
 * <p>
 * {@code oversample} is how a cheap baseline gets close to exact: the mapping's usual oversample of 3 rescores only the quantized top
 * {@code 3k}, which can drop a document whose true rank is inside {@code k} (on one 2000-query sweep, 31 of the exact top-100
 * documents), while an oversample of 10 dropped none. Unset, the field mapping's own setting applies. A baseline oversample also
 * re-enables fidelity on a field whose mapping has rescoring off, since that run's scores are real again.
 * <p>
 * Each candidate reports {@code recall} -- the mean over queries of recall of the <em>baseline's</em> top-k, so the {@code baseline}
 * object in the response is the referent and not a set of relevance judgements -- plus a {@code recall_stats} distribution of the
 * per-query values and a {@code recall_histogram} (the percentiles tie heavily because per-query recall is quantised). The histogram
 * reports every distinct observed value while {@code k} is at most 20; past that the values are dense enough that they are grouped into
 * bins. When {@code recall_histogram_bin_width} is present, each {@code recall} is the lower edge of a half-open bin
 * {@code [recall, recall + width)} -- the same convention as the {@code histogram} aggregation's {@code key} -- except 1.0, which is
 * exact so that "perfect" is never merged with "nearly perfect". The bin width field is absent when the values are exact.
 * <p>
 * Each candidate also reports its {@code vector_ops} distribution and its {@code took_ms} distribution, so a sweep yields the
 * recall/cost curve directly. All configurations run against one point-in-time, so a concurrent refresh cannot masquerade as
 * a recall difference. Note that {@code took_ms} reflects the node's cache state: the baseline pass runs first and is the most
 * exhaustive, so candidates search an index it has already warmed. {@code vector_ops} is the cache-independent cost axis.
 * The query set is either sampled from the indexed documents as above, or supplied explicitly:
 * <pre>
 *   "queries": [ { "id": "q1", "query_vector": [0.1, 0.2] } ]
 * </pre>
 * A {@code query_vector} may also be a hex or base64 encoded string, exactly as in a {@code knn} search section:
 * <pre>
 *   "queries": [ { "id": "q1", "query_vector": "P8AAAMAgAAA=" } ]
 * </pre>
 */
@ServerlessScope(Scope.PUBLIC)
public class RestKnnEvalAction extends BaseRestHandler {

    public static final String ENDPOINT = "_knn_eval";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/" + ENDPOINT), new Route(POST, "/{index}/" + ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        KnnEvalRequest knnEvalRequest = new KnnEvalRequest();
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            knnEvalRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            knnEvalRequest.indicesOptions(IndicesOptions.fromRequest(request, knnEvalRequest.indicesOptions()));
            knnEvalRequest.setKnnEvalSpec(KnnEvalSpec.parse(parser));
        }
        return channel -> client.execute(RankEvalPlugin.KNN_EVAL_ACTION, knnEvalRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "knn_eval_action";
    }
}
