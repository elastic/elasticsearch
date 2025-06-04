/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.graph.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.graph.Connection;
import org.elasticsearch.protocol.xpack.graph.Connection.ConnectionId;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest.TermBoost;
import org.elasticsearch.protocol.xpack.graph.GraphExploreResponse;
import org.elasticsearch.protocol.xpack.graph.Hop;
import org.elasticsearch.protocol.xpack.graph.Vertex;
import org.elasticsearch.protocol.xpack.graph.Vertex.VertexId;
import org.elasticsearch.protocol.xpack.graph.VertexRequest;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.graph.Graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Performs a series of elasticsearch queries and aggregations to explore
 * connected terms in a single index.
 */
public class TransportGraphExploreAction extends HandledTransportAction<GraphExploreRequest, GraphExploreResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGraphExploreAction.class);

    private final ThreadPool threadPool;
    private final NodeClient client;
    protected final XPackLicenseState licenseState;

    static class VertexPriorityQueue extends PriorityQueue<Vertex> {

        VertexPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(Vertex a, Vertex b) {
            return a.getWeight() < b.getWeight();
        }

    }

    @Inject
    public TransportGraphExploreAction(
        ThreadPool threadPool,
        NodeClient client,
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState
    ) {
        super(GraphExploreAction.NAME, transportService, actionFilters, GraphExploreRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.client = client;
        this.licenseState = licenseState;
    }

    @Override
    protected void doExecute(Task task, GraphExploreRequest request, ActionListener<GraphExploreResponse> listener) {
        if (Graph.GRAPH_FEATURE.check(licenseState)) {
            new AsyncGraphAction(request, listener).start();
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.GRAPH));
        }
    }

    class AsyncGraphAction {

        private final GraphExploreRequest request;
        private final ActionListener<GraphExploreResponse> listener;

        private final long startTime;
        private volatile ShardOperationFailedException[] shardFailures;
        private Map<VertexId, Vertex> vertices = new HashMap<>();
        private Map<ConnectionId, Connection> connections = new HashMap<>();

        // Each "hop" is recorded here using hopNumber->fieldName->vertices
        private Map<Integer, Map<String, Set<Vertex>>> hopFindings = new HashMap<>();
        private int currentHopNumber = 0;

        AsyncGraphAction(GraphExploreRequest request, ActionListener<GraphExploreResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.startTime = threadPool.relativeTimeInMillis();
            this.shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        }

        private Vertex getVertex(String field, String term) {
            return vertices.get(Vertex.createId(field, term));
        }

        private Connection addConnection(Vertex from, Vertex to, double weight, long docCount) {
            Connection connection = new Connection(from, to, weight, docCount);
            connections.put(connection.getId(), connection);
            return connection;
        }

        private Vertex addVertex(String field, String term, double score, int depth, long bg, long fg) {
            VertexId key = Vertex.createId(field, term);
            Vertex vertex = vertices.get(key);
            if (vertex == null) {
                vertex = new Vertex(field, term, score, depth, bg, fg);
                vertices.put(key, vertex);
                Map<String, Set<Vertex>> currentWave = hopFindings.get(currentHopNumber);
                if (currentWave == null) {
                    currentWave = new HashMap<>();
                    hopFindings.put(currentHopNumber, currentWave);
                }
                Set<Vertex> verticesForField = currentWave.get(field);
                if (verticesForField == null) {
                    verticesForField = new HashSet<>();
                    currentWave.put(field, verticesForField);
                }
                verticesForField.add(vertex);
            }
            return vertex;
        }

        private void removeVertex(Vertex vertex) {
            vertices.remove(vertex.getId());
            hopFindings.get(currentHopNumber).get(vertex.getField()).remove(vertex);
        }

        /**
         * Step out from some existing vertex terms looking for useful
         * connections
         *
         * @param timedOut the value of timedOut field in the search response
         */
        synchronized void expand(boolean timedOut) {
            Map<String, Set<Vertex>> lastHopFindings = hopFindings.get(currentHopNumber);
            if ((currentHopNumber >= (request.getHopNumbers() - 1)) || (lastHopFindings == null) || (lastHopFindings.size() == 0)) {
                // Either we gathered no leads from the last hop or we have
                // reached the final hop
                listener.onResponse(buildResponse(timedOut));
                return;
            }
            Hop lastHop = request.getHop(currentHopNumber);
            currentHopNumber++;
            Hop currentHop = request.getHop(currentHopNumber);

            final SearchRequest searchRequest = new SearchRequest(request.indices()).indicesOptions(request.indicesOptions());
            if (request.routing() != null) {
                searchRequest.routing(request.routing());
            }

            BoolQueryBuilder rootBool = QueryBuilders.boolQuery();

            // A single sample pool of docs is built at the root of the aggs tree.
            // For quality's sake it might have made more sense to sample top docs
            // for each of the terms from the previous hop (e.g. an initial query for "beatles"
            // may have separate doc-sample pools for significant root terms "john", "paul", "yoko" etc)
            // but I found this dramatically slowed down execution - each pool typically had different docs which
            // each had non-overlapping sets of terms that needed frequencies looking up for significant terms.
            // A common sample pool reduces the specialization that can be given to each root term but
            // ultimately is much faster to run because of the shared vocabulary in a single sample set.
            AggregationBuilder sampleAgg = null;
            if (request.sampleDiversityField() != null) {
                DiversifiedAggregationBuilder diversifiedSampleAgg = AggregationBuilders.diversifiedSampler("sample")
                    .shardSize(request.sampleSize());
                diversifiedSampleAgg.field(request.sampleDiversityField());
                diversifiedSampleAgg.maxDocsPerValue(request.maxDocsPerDiversityValue());
                sampleAgg = diversifiedSampleAgg;
            } else {
                sampleAgg = AggregationBuilders.sampler("sample").shardSize(request.sampleSize());
            }

            // Add any user-supplied criteria to the root query as a must clause
            rootBool.must(currentHop.guidingQuery());

            // Build a MUST clause that matches one of either
            // a:) include clauses supplied by the client or
            // b:) vertex terms from the previous hop.
            BoolQueryBuilder sourceTermsOrClause = QueryBuilders.boolQuery();
            addUserDefinedIncludesToQuery(currentHop, sourceTermsOrClause);
            addBigOrClause(lastHopFindings, sourceTermsOrClause);

            rootBool.must(sourceTermsOrClause);

            // Now build the agg tree that will channel the content ->
            // base agg is terms agg for terms from last wave (one per field),
            // under each is a sig_terms agg to find next candidates (again, one per field)...
            for (int fieldNum = 0; fieldNum < lastHop.getNumberVertexRequests(); fieldNum++) {
                VertexRequest lastVr = lastHop.getVertexRequest(fieldNum);
                Set<Vertex> lastWaveVerticesForField = lastHopFindings.get(lastVr.fieldName());
                if (lastWaveVerticesForField == null) {
                    continue;
                }
                SortedSet<BytesRef> terms = new TreeSet<>();
                for (Vertex v : lastWaveVerticesForField) {
                    terms.add(new BytesRef(v.getTerm()));
                }
                TermsAggregationBuilder lastWaveTermsAgg = AggregationBuilders.terms("field" + fieldNum)
                    .includeExclude(new IncludeExclude(null, null, terms, null))
                    .shardMinDocCount(1)
                    .field(lastVr.fieldName())
                    .minDocCount(1)
                    // Map execution mode used because Sampler agg keeps us
                    // focused on smaller sets of high quality docs and therefore
                    // examine smaller volumes of terms
                    .executionHint("map")
                    .size(terms.size());
                sampleAgg.subAggregation(lastWaveTermsAgg);
                for (int f = 0; f < currentHop.getNumberVertexRequests(); f++) {
                    VertexRequest vr = currentHop.getVertexRequest(f);
                    int size = vr.size();
                    if (vr.fieldName().equals(lastVr.fieldName())) {
                        // We have the potential for self-loops as we are looking at the same field so add 1 to the requested size
                        // because we need to eliminate fieldA:termA -> fieldA:termA links that are likely to be in the results.
                        size++;
                    }
                    if (request.useSignificance()) {
                        SignificantTermsAggregationBuilder nextWaveSigTerms = AggregationBuilders.significantTerms("field" + f)
                            .field(vr.fieldName())
                            .minDocCount(vr.minDocCount())
                            .shardMinDocCount(vr.shardMinDocCount())
                            .executionHint("map")
                            .size(size);
                        // nextWaveSigTerms.significanceHeuristic(new PercentageScore.PercentageScoreBuilder());
                        // Had some issues with no significant terms being returned when asking for small
                        // number of final results (eg 1) and only one shard. Setting shard_size higher helped.
                        if (size < 10) {
                            nextWaveSigTerms.shardSize(10);
                        }
                        // Alternative choices of significance algo didn't seem to be improvements....
                        // nextWaveSigTerms.significanceHeuristic(new GND.GNDBuilder(true));
                        // nextWaveSigTerms.significanceHeuristic(new ChiSquare.ChiSquareBuilder(false, true));

                        if (vr.hasIncludeClauses()) {
                            SortedSet<BytesRef> includes = vr.includeValuesAsSortedSet();
                            nextWaveSigTerms.includeExclude(new IncludeExclude(null, null, includes, null));
                            // Originally I thought users would always want the
                            // same number of results as listed in the include
                            // clause but it may be the only want the most
                            // significant e.g. in the lastfm example of
                            // plotting a single user's tastes and how that maps
                            // into a network showing only the most interesting
                            // band connections. So line below commmented out

                            // nextWaveSigTerms.size(includes.length);

                        } else if (vr.hasExcludeClauses()) {
                            nextWaveSigTerms.includeExclude(new IncludeExclude(null, null, null, vr.excludesAsSortedSet()));
                        }
                        lastWaveTermsAgg.subAggregation(nextWaveSigTerms);
                    } else {
                        TermsAggregationBuilder nextWavePopularTerms = AggregationBuilders.terms("field" + f)
                            .field(vr.fieldName())
                            .minDocCount(vr.minDocCount())
                            .shardMinDocCount(vr.shardMinDocCount())
                            // Map execution mode used because Sampler agg keeps us
                            // focused on smaller sets of high quality docs and therefore
                            // examine smaller volumes of terms
                            .executionHint("map")
                            .size(size);
                        if (vr.hasIncludeClauses()) {
                            SortedSet<BytesRef> includes = vr.includeValuesAsSortedSet();
                            nextWavePopularTerms.includeExclude(new IncludeExclude(null, null, includes, null));
                            // nextWavePopularTerms.size(includes.length);
                        } else if (vr.hasExcludeClauses()) {
                            nextWavePopularTerms.includeExclude(new IncludeExclude(null, null, null, vr.excludesAsSortedSet()));
                        }
                        lastWaveTermsAgg.subAggregation(nextWavePopularTerms);
                    }
                }
            }

            // Execute the search
            SearchSourceBuilder source = new SearchSourceBuilder().query(rootBool).aggregation(sampleAgg).size(0);
            if (request.timeout() != null) {
                // Actual resolution of timer is granularity of the interval
                // configured globally for updating estimated time.
                long timeRemainingMillis = startTime + request.timeout().millis() - threadPool.relativeTimeInMillis();
                if (timeRemainingMillis <= 0) {
                    listener.onResponse(buildResponse(true));
                    return;
                }

                source.timeout(TimeValue.timeValueMillis(timeRemainingMillis));
            }
            searchRequest.source(source);

            logger.trace("executing expansion graph search request");
            client.search(searchRequest, new DelegatingActionListener<>(listener) {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    addShardFailures(searchResponse.getShardFailures());

                    ArrayList<Connection> newConnections = new ArrayList<Connection>();
                    ArrayList<Vertex> newVertices = new ArrayList<Vertex>();
                    SingleBucketAggregation sample = searchResponse.getAggregations().get("sample");

                    // We think of the total scores as the energy-level pouring
                    // out of all the last hop's connections.
                    // Each new node encountered is given a score which is
                    // normalized between zero and one based on
                    // what percentage of the total scores its own score
                    // provides
                    double totalSignalOutput = getExpandTotalSignalStrength(lastHop, currentHop, sample);

                    // Signal output can be zero if we did not encounter any new
                    // terms as part of this stage
                    if (totalSignalOutput > 0) {
                        addAndScoreNewVertices(lastHop, currentHop, sample, totalSignalOutput, newConnections, newVertices);

                        trimNewAdditions(currentHop, newConnections, newVertices);
                    }

                    // Potentially run another round of queries to perform next"hop" - will terminate if no new additions
                    expand(searchResponse.isTimedOut());

                }

                // Add new vertices and apportion share of total signal along
                // connections
                private void addAndScoreNewVertices(
                    Hop lastHop,
                    Hop currentHop,
                    SingleBucketAggregation sample,
                    double totalSignalOutput,
                    ArrayList<Connection> newConnections,
                    ArrayList<Vertex> newVertices
                ) {
                    // Gather all matching terms into the graph and propagate
                    // signals
                    for (int j = 0; j < lastHop.getNumberVertexRequests(); j++) {
                        VertexRequest lastVr = lastHop.getVertexRequest(j);
                        Terms lastWaveTerms = sample.getAggregations().get("field" + j);
                        if (lastWaveTerms == null) {
                            // There were no terms from the previous phase that needed pursuing
                            continue;
                        }
                        List<? extends Terms.Bucket> buckets = lastWaveTerms.getBuckets();
                        for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket lastWaveTerm : buckets) {
                            Vertex fromVertex = getVertex(lastVr.fieldName(), lastWaveTerm.getKeyAsString());
                            for (int k = 0; k < currentHop.getNumberVertexRequests(); k++) {
                                VertexRequest vr = currentHop.getVertexRequest(k);
                                // As we travel further out into the graph we apply a
                                // decay to the signals being propagated down the various channels.
                                double decay = 0.95d;
                                if (request.useSignificance()) {
                                    SignificantTerms significantTerms = lastWaveTerm.getAggregations().get("field" + k);
                                    if (significantTerms != null) {
                                        for (Bucket bucket : significantTerms.getBuckets()) {
                                            if ((vr.fieldName().equals(fromVertex.getField()))
                                                && (bucket.getKeyAsString().equals(fromVertex.getTerm()))) {
                                                // Avoid self-joins
                                                continue;
                                            }
                                            double signalStrength = bucket.getSignificanceScore() / totalSignalOutput;

                                            // Decay the signal by the weight attached to the source vertex
                                            signalStrength = signalStrength * Math.min(decay, fromVertex.getWeight());

                                            Vertex toVertex = getVertex(vr.fieldName(), bucket.getKeyAsString());
                                            if (toVertex == null) {
                                                toVertex = addVertex(
                                                    vr.fieldName(),
                                                    bucket.getKeyAsString(),
                                                    signalStrength,
                                                    currentHopNumber,
                                                    bucket.getSupersetDf(),
                                                    bucket.getSubsetDf()
                                                );
                                                newVertices.add(toVertex);
                                            } else {
                                                toVertex.setWeight(toVertex.getWeight() + signalStrength);
                                                // We cannot (without further querying) determine an accurate number
                                                // for the foreground count of the toVertex term - if we sum the values
                                                // from each fromVertex term we may actually double-count occurrences so
                                                // the best we can do is take the maximum foreground value we have observed
                                                toVertex.setFg(Math.max(toVertex.getFg(), bucket.getSubsetDf()));
                                            }
                                            newConnections.add(addConnection(fromVertex, toVertex, signalStrength, bucket.getDocCount()));
                                        }
                                    }
                                } else {
                                    Terms terms = lastWaveTerm.getAggregations().get("field" + k);
                                    if (terms != null) {
                                        for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket : terms.getBuckets()) {
                                            double signalStrength = bucket.getDocCount() / totalSignalOutput;
                                            // Decay the signal by the weight attached to the source vertex
                                            signalStrength = signalStrength * Math.min(decay, fromVertex.getWeight());

                                            Vertex toVertex = getVertex(vr.fieldName(), bucket.getKeyAsString());
                                            if (toVertex == null) {
                                                toVertex = addVertex(
                                                    vr.fieldName(),
                                                    bucket.getKeyAsString(),
                                                    signalStrength,
                                                    currentHopNumber,
                                                    0,
                                                    0
                                                );
                                                newVertices.add(toVertex);
                                            } else {
                                                toVertex.setWeight(toVertex.getWeight() + signalStrength);
                                            }
                                            newConnections.add(addConnection(fromVertex, toVertex, signalStrength, bucket.getDocCount()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Having let the signals from the last results rattle around the graph
                // we have adjusted weights for the various vertices we encountered.
                // Now we review these new additions and remove those with the
                // weakest weights.
                // A priority queue is used to trim vertices according to the size settings
                // requested for each field.
                private void trimNewAdditions(Hop currentHop, ArrayList<Connection> newConnections, ArrayList<Vertex> newVertices) {
                    Set<Vertex> evictions = new HashSet<>();

                    for (int k = 0; k < currentHop.getNumberVertexRequests(); k++) {
                        // For each of the fields
                        VertexRequest vr = currentHop.getVertexRequest(k);
                        if (newVertices.size() <= vr.size()) {
                            // Nothing to trim
                            continue;
                        }
                        // Get the top vertices for this field
                        VertexPriorityQueue pq = new VertexPriorityQueue(vr.size());
                        for (Vertex vertex : newVertices) {
                            if (vertex.getField().equals(vr.fieldName())) {
                                Vertex eviction = pq.insertWithOverflow(vertex);
                                if (eviction != null) {
                                    evictions.add(eviction);
                                }
                            }
                        }
                    }
                    // Remove weak new nodes and their dangling connections from the main graph
                    if (evictions.size() > 0) {
                        for (Connection connection : newConnections) {
                            if (evictions.contains(connection.getTo())) {
                                connections.remove(connection.getId());
                                removeVertex(connection.getTo());
                            }
                        }
                    }
                }
                // TODO right now we only trim down to the best N vertices. We might also want to offer
                // clients the option to limit to the best M connections. One scenario where this is required
                // is if the "from" and "to" nodes are a client-supplied set of includes e.g. a list of
                // music artists then the client may be wanting to draw only the most-interesting connections
                // between them. See https://github.com/elastic/x-plugins/issues/518#issuecomment-160186424
                // I guess clients could trim the returned connections (which all have weights) but I wonder if
                // we can do something server-side here

                // Helper method - compute the total signal of all scores in the search results
                private double getExpandTotalSignalStrength(Hop lastHop, Hop currentHop, SingleBucketAggregation sample) {
                    double totalSignalOutput = 0;
                    for (int j = 0; j < lastHop.getNumberVertexRequests(); j++) {
                        VertexRequest lastVr = lastHop.getVertexRequest(j);
                        Terms lastWaveTerms = sample.getAggregations().get("field" + j);
                        if (lastWaveTerms == null) {
                            continue;
                        }
                        List<? extends Terms.Bucket> buckets = lastWaveTerms.getBuckets();
                        for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket lastWaveTerm : buckets) {
                            for (int k = 0; k < currentHop.getNumberVertexRequests(); k++) {
                                VertexRequest vr = currentHop.getVertexRequest(k);
                                if (request.useSignificance()) {
                                    // Signal is based on significance score
                                    SignificantTerms significantTerms = lastWaveTerm.getAggregations().get("field" + k);
                                    if (significantTerms != null) {
                                        for (Bucket bucket : significantTerms.getBuckets()) {
                                            if ((vr.fieldName().equals(lastVr.fieldName()))
                                                && (bucket.getKeyAsString().equals(lastWaveTerm.getKeyAsString()))) {
                                                // don't count self joins (term A obviously co-occurs with term A)
                                                continue;
                                            } else {
                                                totalSignalOutput += bucket.getSignificanceScore();
                                            }
                                        }
                                    }
                                } else {
                                    // Signal is based on popularity (number of
                                    // documents)
                                    Terms terms = lastWaveTerm.getAggregations().get("field" + k);
                                    if (terms != null) {
                                        for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket : terms.getBuckets()) {
                                            if ((vr.fieldName().equals(lastVr.fieldName()))
                                                && (bucket.getKeyAsString().equals(lastWaveTerm.getKeyAsString()))) {
                                                // don't count self joins (term A obviously co-occurs with term A)
                                                continue;
                                            } else {
                                                totalSignalOutput += bucket.getDocCount();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return totalSignalOutput;
                }
            });
        }

        private static void addUserDefinedIncludesToQuery(Hop hop, BoolQueryBuilder sourceTermsOrClause) {
            for (int i = 0; i < hop.getNumberVertexRequests(); i++) {
                VertexRequest vr = hop.getVertexRequest(i);
                if (vr.hasIncludeClauses()) {
                    addNormalizedBoosts(sourceTermsOrClause, vr);
                }
            }
        }

        private static void addBigOrClause(Map<String, Set<Vertex>> lastHopFindings, BoolQueryBuilder sourceTermsOrClause) {
            int numClauses = sourceTermsOrClause.should().size();
            for (Entry<String, Set<Vertex>> entry : lastHopFindings.entrySet()) {
                numClauses += entry.getValue().size();
            }
            if (numClauses < IndexSearcher.getMaxClauseCount()) {
                // We can afford to build a Boolean OR query with individual
                // boosts for interesting terms
                for (Entry<String, Set<Vertex>> entry : lastHopFindings.entrySet()) {
                    for (Vertex vertex : entry.getValue()) {
                        sourceTermsOrClause.should(
                            QueryBuilders.constantScoreQuery(QueryBuilders.termQuery(vertex.getField(), vertex.getTerm()))
                                .boost((float) vertex.getWeight())
                        );
                    }
                }

            } else {
                // Too many terms - we need a cheaper form of query to execute this
                for (Entry<String, Set<Vertex>> entry : lastHopFindings.entrySet()) {
                    List<String> perFieldTerms = new ArrayList<>();
                    for (Vertex vertex : entry.getValue()) {
                        perFieldTerms.add(vertex.getTerm());
                    }
                    sourceTermsOrClause.should(QueryBuilders.constantScoreQuery(QueryBuilders.termsQuery(entry.getKey(), perFieldTerms)));
                }
            }
        }

        /**
         * For a given root query (or a set of "includes" root constraints) find
         * the related terms. These will be our start points in the graph
         * navigation.
         */
        public synchronized void start() {
            try {

                final SearchRequest searchRequest = new SearchRequest(request.indices()).indicesOptions(request.indicesOptions());
                if (request.routing() != null) {
                    searchRequest.routing(request.routing());
                }

                BoolQueryBuilder rootBool = QueryBuilders.boolQuery();

                AggregationBuilder rootSampleAgg = null;
                if (request.sampleDiversityField() != null) {
                    DiversifiedAggregationBuilder diversifiedRootSampleAgg = AggregationBuilders.diversifiedSampler("sample")
                        .shardSize(request.sampleSize());
                    diversifiedRootSampleAgg.field(request.sampleDiversityField());
                    diversifiedRootSampleAgg.maxDocsPerValue(request.maxDocsPerDiversityValue());
                    rootSampleAgg = diversifiedRootSampleAgg;
                } else {
                    rootSampleAgg = AggregationBuilders.sampler("sample").shardSize(request.sampleSize());
                }

                Hop rootHop = request.getHop(0);

                // Add any user-supplied criteria to the root query as a should clause
                rootBool.must(rootHop.guidingQuery());

                // If any of the root terms have an "include" restriction then
                // we add a root-level MUST clause that
                // mandates that at least one of the potentially many terms of
                // interest must be matched (using a should array)
                BoolQueryBuilder includesContainer = QueryBuilders.boolQuery();
                addUserDefinedIncludesToQuery(rootHop, includesContainer);
                if (includesContainer.should().size() > 0) {
                    rootBool.must(includesContainer);
                }

                for (int i = 0; i < rootHop.getNumberVertexRequests(); i++) {
                    VertexRequest vr = rootHop.getVertexRequest(i);
                    if (request.useSignificance()) {
                        SignificantTermsAggregationBuilder sigBuilder = AggregationBuilders.significantTerms("field" + i);
                        sigBuilder.field(vr.fieldName())
                            .shardMinDocCount(vr.shardMinDocCount())
                            .minDocCount(vr.minDocCount())
                            // Map execution mode used because Sampler agg
                            // keeps us focused on smaller sets of high quality
                            // docs and therefore examine smaller volumes of terms
                            .executionHint("map")
                            .size(vr.size());
                        // It is feasible that clients could provide a choice of
                        // significance heuristic at some point e.g:
                        // sigBuilder.significanceHeuristic(new
                        // PercentageScore.PercentageScoreBuilder());

                        if (vr.hasIncludeClauses()) {
                            SortedSet<BytesRef> includes = vr.includeValuesAsSortedSet();
                            sigBuilder.includeExclude(new IncludeExclude(null, null, includes, null));
                            sigBuilder.size(includes.size());
                        }
                        if (vr.hasExcludeClauses()) {
                            sigBuilder.includeExclude(new IncludeExclude(null, null, null, vr.excludesAsSortedSet()));
                        }
                        rootSampleAgg.subAggregation(sigBuilder);
                    } else {
                        TermsAggregationBuilder termsBuilder = AggregationBuilders.terms("field" + i);
                        // Min doc count etc really only applies when we are
                        // thinking about certainty of significance scores -
                        // perhaps less necessary when considering popularity
                        // termsBuilder.field(vr.fieldName()).shardMinDocCount(shardMinDocCount)
                        // .minDocCount(minDocCount).executionHint("map").size(vr.size());
                        termsBuilder.field(vr.fieldName()).executionHint("map").size(vr.size());
                        if (vr.hasIncludeClauses()) {
                            SortedSet<BytesRef> includes = vr.includeValuesAsSortedSet();
                            termsBuilder.includeExclude(new IncludeExclude(null, null, includes, null));
                            termsBuilder.size(includes.size());
                        }
                        if (vr.hasExcludeClauses()) {
                            termsBuilder.includeExclude(new IncludeExclude(null, null, null, vr.excludesAsSortedSet()));
                        }
                        rootSampleAgg.subAggregation(termsBuilder);
                    }
                }

                // Run the search
                SearchSourceBuilder source = new SearchSourceBuilder().query(rootBool).aggregation(rootSampleAgg).size(0);
                if (request.timeout() != null) {
                    source.timeout(request.timeout());
                }
                searchRequest.source(source);
                logger.trace("executing initial graph search request");
                client.search(searchRequest, new DelegatingActionListener<>(listener) {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        addShardFailures(searchResponse.getShardFailures());
                        SingleBucketAggregation sample = searchResponse.getAggregations().get("sample");

                        // Determine the total scores for all interesting terms
                        double totalSignalStrength = getInitialTotalSignalStrength(rootHop, sample);

                        // Now gather the best matching terms and compute signal weight according to their
                        // share of the total signal strength
                        for (int j = 0; j < rootHop.getNumberVertexRequests(); j++) {
                            VertexRequest vr = rootHop.getVertexRequest(j);
                            if (request.useSignificance()) {
                                SignificantTerms significantTerms = sample.getAggregations().get("field" + j);
                                List<? extends Bucket> buckets = significantTerms.getBuckets();
                                for (Bucket bucket : buckets) {
                                    double signalWeight = bucket.getSignificanceScore() / totalSignalStrength;
                                    addVertex(
                                        vr.fieldName(),
                                        bucket.getKeyAsString(),
                                        signalWeight,
                                        currentHopNumber,
                                        bucket.getSupersetDf(),
                                        bucket.getSubsetDf()
                                    );
                                }
                            } else {
                                Terms terms = sample.getAggregations().get("field" + j);
                                List<? extends Terms.Bucket> buckets = terms.getBuckets();
                                for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket : buckets) {
                                    double signalWeight = bucket.getDocCount() / totalSignalStrength;
                                    addVertex(vr.fieldName(), bucket.getKeyAsString(), signalWeight, currentHopNumber, 0, 0);
                                }
                            }
                        }
                        // Expand out from these root vertices looking for connections with other terms
                        expand(searchResponse.isTimedOut());

                    }

                    // Helper method - Provides a total signal strength for all terms connected to the initial query
                    private double getInitialTotalSignalStrength(Hop rootHop, SingleBucketAggregation sample) {
                        double totalSignalStrength = 0;
                        for (int i = 0; i < rootHop.getNumberVertexRequests(); i++) {
                            if (request.useSignificance()) {
                                // Signal is based on significance score
                                SignificantTerms significantTerms = sample.getAggregations().get("field" + i);
                                List<? extends Bucket> buckets = significantTerms.getBuckets();
                                for (Bucket bucket : buckets) {
                                    totalSignalStrength += bucket.getSignificanceScore();
                                }
                            } else {
                                // Signal is based on popularity (number of documents)
                                Terms terms = sample.getAggregations().get("field" + i);
                                List<? extends Terms.Bucket> buckets = terms.getBuckets();
                                for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket : buckets) {
                                    totalSignalStrength += bucket.getDocCount();
                                }
                            }
                        }
                        return totalSignalStrength;
                    }
                });
            } catch (Exception e) {
                logger.error("unable to execute the graph query", e);
                listener.onFailure(e);
            }
        }

        private static void addNormalizedBoosts(BoolQueryBuilder includesContainer, VertexRequest vr) {
            TermBoost[] termBoosts = vr.includeValues();

            if ((includesContainer.should().size() + termBoosts.length) > IndexSearcher.getMaxClauseCount()) {
                // Too many terms - we need a cheaper form of query to execute this
                List<String> termValues = new ArrayList<>();
                for (TermBoost tb : termBoosts) {
                    termValues.add(tb.getTerm());
                }
                includesContainer.should(QueryBuilders.constantScoreQuery(QueryBuilders.termsQuery(vr.fieldName(), termValues)));
                return;

            }
            // We have a sufficiently low number of terms to use the per-term boosts.
            // Lucene boosts are >=1 so we baseline the provided boosts to start
            // from 1
            float minBoost = Float.MAX_VALUE;
            for (TermBoost tb : termBoosts) {
                minBoost = Math.min(minBoost, tb.getBoost());
            }
            for (TermBoost tb : termBoosts) {
                float normalizedBoost = tb.getBoost() / minBoost;
                includesContainer.should(QueryBuilders.termQuery(vr.fieldName(), tb.getTerm()).boost(normalizedBoost));
            }
        }

        void addShardFailures(ShardOperationFailedException[] failures) {
            if (CollectionUtils.isEmpty(failures) == false) {
                ShardOperationFailedException[] duplicates = new ShardOperationFailedException[shardFailures.length + failures.length];
                System.arraycopy(shardFailures, 0, duplicates, 0, shardFailures.length);
                System.arraycopy(failures, 0, duplicates, shardFailures.length, failures.length);
                shardFailures = ExceptionsHelper.groupBy(duplicates);
            }
        }

        protected GraphExploreResponse buildResponse(boolean timedOut) {
            long took = threadPool.relativeTimeInMillis() - startTime;
            return new GraphExploreResponse(took, timedOut, shardFailures, vertices, connections, request.returnDetailedInfo());
        }

    }
}
