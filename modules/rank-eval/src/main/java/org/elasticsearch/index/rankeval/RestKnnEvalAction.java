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
 *   "knn_settings": [ { "visit_percentage": 2 }, { "visit_percentage": 5 } ],
 *   "filter": { "term": { "tenant": "acme" } },
 *   "max_queries_per_batch": 50,
 *   "max_concurrent_searches": 1,
 *   "include_details": false,
 *   "include_histogram": false,
 *   "include_fidelity": false
 * }
 * </pre>
 * <ul>
 * <li>{@code queries} or {@code sample}, exactly one; a sampled document is dropped from both hit lists so a self-hit cannot
 *     inflate recall by {@code 1/k}. {@code query_vector} is a float array or a hex/base64 string, as in a {@code knn} section.</li>
 * <li>{@code baseline} defaults to {@code exact: true}: a full-precision scan of every document, O(N) per query, gated by
 *     {@code search.allow_expensive_queries}. A cheaper baseline is any knob set; its {@code oversample} is what bounds recall
 *     ({@code rescore_window = min(k * oversample, 10000)}), so {@code exact} is the only certifiable baseline at large {@code k}.</li>
 * <li>{@code knn_settings} take {@code visit_percentage} ({@code bbq_disk} only), {@code num_candidates} (any type) and
 *     {@code oversample} (quantized types). A knob the field type would ignore is rejected, since a plain {@code knn} search
 *     silently ignores it and a sweep over it reads as recall 1.0.</li>
 * <li>{@code filter} applies to every evaluation search, not to sampling.</li>
 * <li>{@code recall} is recall of the <em>baseline's</em> top-k, never of relevance judgements. Each knob set echoes the
 *     {@code rescore_window} and {@code effective_num_candidates} the search actually used.</li>
 * <li>{@code vector_ops} is the load-independent cost axis; {@code took_ms} is shard-side {@code took} on an index the baseline pass
 *     has already warmed. An exact baseline's ops are float32 comparisons over every live vector ({@code baseline_vector_ops_kind}).</li>
 * <li>Opt-in: {@code include_histogram}, {@code include_details} (per-query hits with {@code baseline_rank}, {@code missed}),
 *     {@code include_fidelity} (value-based {@code recall_value}, profile-fidelity {@code epsilon}).</li>
 * <li>{@code environment} reports the segment layout, field configuration, index version and
 *     {@code search.allow_expensive_queries} the sweep ran against, since a fixed {@code visit_percentage} means something different
 *     on one segment than on twenty. Its {@code index} and {@code index_version_created} parts need {@code monitor}.</li>
 * </ul>
 * The knob checks, echoed windows and fidelity metrics read the field mapping and are skipped without {@code view_index_metadata};
 * the monitor-privileged parts of {@code environment} are likewise dropped rather than failing the request.
 */
// operator-only until the API is reviewed
@ServerlessScope(Scope.INTERNAL)
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
