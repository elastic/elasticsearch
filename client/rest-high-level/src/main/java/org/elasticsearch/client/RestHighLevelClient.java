/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.cluster.ClusterClient;
import org.elasticsearch.client.graph.GraphClient;
import org.elasticsearch.client.index.IndicesClient;
import org.elasticsearch.client.ingest.IngestClient;
import org.elasticsearch.client.license.LicenseClient;
import org.elasticsearch.client.migration.MigrationClient;
import org.elasticsearch.client.ml.MachineLearningClient;
import org.elasticsearch.client.rollup.RollupClient;
import org.elasticsearch.client.security.SecurityClient;
import org.elasticsearch.client.snapshot.SnapshotClient;
import org.elasticsearch.client.tasks.TasksClient;
import org.elasticsearch.client.watcher.WatcherClient;
import org.elasticsearch.client.xpack.XPackClient;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * High level REST client that wraps an instance of the low level {@link RestClient} and allows to build requests and read responses.
 * The {@link RestClient} instance is internally built based on the provided {@link RestClientBuilder} and it gets closed automatically
 * when closing the {@link RestHighLevelClient} instance that wraps it.
 * In case an already existing instance of a low-level REST client needs to be provided, this class can be subclassed and the
 * {@link #RestHighLevelClient(RestClient, CheckedConsumer, List)}  constructor can be used.
 * This class can also be sub-classed to expose additional client methods that make use of endpoints added to Elasticsearch through
 * plugins, or to add support for custom response sections, again added to Elasticsearch through plugins.
 */
public class RestHighLevelClient implements Closeable {

    final CoreClient coreClient;
    private final CheckedConsumer<RestClient, IOException> doClose;

    /**
     * Creates a {@link RestHighLevelClient} given the low level {@link RestClientBuilder} that allows to build the
     * {@link RestClient} to be used to perform requests.
     */
    public RestHighLevelClient(RestClientBuilder restClientBuilder) {
        this(restClientBuilder, Collections.emptyList());
    }

    /**
     * Creates a {@link RestHighLevelClient} given the low level {@link RestClientBuilder} that allows to build the
     * {@link RestClient} to be used to perform requests and parsers for custom response sections added to Elasticsearch through plugins.
     */
    protected RestHighLevelClient(RestClientBuilder restClientBuilder, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        this(restClientBuilder.build(), RestClient::close, namedXContentEntries);
    }

    /**
     * Creates a {@link RestHighLevelClient} given the low level {@link RestClient} that it should use to perform requests and
     * a list of entries that allow to parse custom response sections added to Elasticsearch through plugins.
     * This constructor can be called by subclasses in case an externally created low-level REST client needs to be provided.
     * The consumer argument allows to control what needs to be done when the {@link #close()} method is called.
     * Also subclasses can provide parsers for custom response sections added to Elasticsearch through plugins.
     */
    protected RestHighLevelClient(RestClient restClient, CheckedConsumer<RestClient, IOException> doClose,
                                  List<NamedXContentRegistry.Entry> namedXContentEntries) {
        this.doClose = Objects.requireNonNull(doClose, "doClose consumer must not be null");
        this.coreClient = new CoreClient(restClient, namedXContentEntries);
    }

    /**
     * Build an area client.
     */
    protected final <T> T buildSubClient(Function<CoreClient, T> ctor) {
        return ctor.apply(coreClient);
    }

    /**
     * Returns the low-level client that the current high-level client instance is using to perform requests
     */
    public final RestClient getLowLevelClient() {
        return coreClient.client;
    }

    @Override
    public void close() throws IOException {
        doClose.accept(coreClient.client);
    }

    /**
     * Provides an {@link IndicesClient} which can be used to access the Indices API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices.html">Indices API on elastic.co</a>
     */
    public final IndicesClient indices() {
        return buildSubClient(IndicesClient::new);
    }

    /**
     * Provides a {@link ClusterClient} which can be used to access the Cluster API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster.html">Cluster API on elastic.co</a>
     */
    public final ClusterClient cluster() {
        return buildSubClient(ClusterClient::new);
    }

    /**
     * Provides a {@link IngestClient} which can be used to access the Ingest API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest.html">Ingest API on elastic.co</a>
     */
    public final IngestClient ingest() {
        return buildSubClient(IngestClient::new);
    }

    /**
     * Provides a {@link SnapshotClient} which can be used to access the Snapshot API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html">Snapshot API on elastic.co</a>
     */
    public final SnapshotClient snapshot() {
        return buildSubClient(SnapshotClient::new);
    }

    /**
     * Provides methods for accessing the Elastic Licensed Rollup APIs that
     * are shipped with the default distribution of Elasticsearch. All of
     * these APIs will 404 if run against the OSS distribution of Elasticsearch.
     * <p>
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/rollup-apis.html">
     * Watcher APIs on elastic.co</a> for more information.
     */
    public RollupClient rollup() {
        return buildSubClient(RollupClient::new);
    }

    /**
     * Provides a {@link TasksClient} which can be used to access the Tasks API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html">Task Management API on elastic.co</a>
     */
    public final TasksClient tasks() {
        return buildSubClient(TasksClient::new);
    }

    /**
     * Provides methods for accessing the Elastic Licensed X-Pack Info
     * and Usage APIs that are shipped with the default distribution of
     * Elasticsearch. All of these APIs will 404 if run against the OSS
     * distribution of Elasticsearch.
     * <p>
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/info-api.html">
     * Info APIs on elastic.co</a> for more information.
     */
    public final XPackClient xpack() {
        return buildSubClient(XPackClient::new);
    }

    /**
     * Provides methods for accessing the Elastic Licensed Watcher APIs that
     * are shipped with the default distribution of Elasticsearch. All of
     * these APIs will 404 if run against the OSS distribution of Elasticsearch.
     * <p>
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/watcher-api.html">
     * Watcher APIs on elastic.co</a> for more information.
     */
    public WatcherClient watcher() { return buildSubClient(WatcherClient::new); }

    /**
     * Provides methods for accessing the Elastic Licensed Graph explore API that
     * is shipped with the default distribution of Elasticsearch. All of
     * these APIs will 404 if run against the OSS distribution of Elasticsearch.
     * <p>
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/graph-explore-api.html">
     * Graph API on elastic.co</a> for more information.
     */
    public GraphClient graph() { return buildSubClient(GraphClient::new); }

    /**
     * Provides methods for accessing the Elastic Licensed Licensing APIs that
     * are shipped with the default distribution of Elasticsearch. All of
     * these APIs will 404 if run against the OSS distribution of Elasticsearch.
     * <p>
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/licensing-apis.html">
     * Licensing APIs on elastic.co</a> for more information.
     */
    public LicenseClient license() { return buildSubClient(LicenseClient::new); }

    /**
     * Provides methods for accessing the Elastic Licensed Licensing APIs that
     * are shipped with the default distribution of Elasticsearch. All of
     * these APIs will 404 if run against the OSS distribution of Elasticsearch.
     * <p>
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/migration-api.html">
     * Migration APIs on elastic.co</a> for more information.
     */
    public MigrationClient migration() {
        return buildSubClient(MigrationClient::new);
    }

    /**
     * Provides methods for accessing the Elastic Licensed Machine Learning APIs that
     * are shipped with the Elastic Stack distribution of Elasticsearch. All of
     * these APIs will 404 if run against the OSS distribution of Elasticsearch.
     * <p>
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-apis.html">
     * Machine Learning APIs on elastic.co</a> for more information.
     *
     * @return the client wrapper for making Machine Learning API calls
     */
    public MachineLearningClient machineLearning() {
        return buildSubClient(MachineLearningClient::new);
    }

    /**
     * Provides methods for accessing the Elastic Licensed Security APIs that
     * are shipped with the Elastic Stack distribution of Elasticsearch. All of
     * these APIs will 404 if run against the OSS distribution of Elasticsearch.
     * <p>
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html">
     * Security APIs on elastic.co</a> for more information.
     *
     * @return the client wrapper for making Security API calls
     */
    public SecurityClient security() {
        return buildSubClient(SecurityClient::new);
    }

    /**
     * Executes a bulk request using the Bulk API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">Bulk API on elastic.co</a>
     * @param bulkRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final BulkResponse bulk(BulkRequest bulkRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(bulkRequest, RequestConverters::bulk, options, BulkResponse::fromXContent,
            emptySet());
    }

    /**
     * Asynchronously executes a bulk request using the Bulk API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">Bulk API on elastic.co</a>
     * @param bulkRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void bulkAsync(BulkRequest bulkRequest, RequestOptions options, ActionListener<BulkResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(bulkRequest, RequestConverters::bulk, options, BulkResponse::fromXContent, listener,
            emptySet());
    }

    /**
     * Executes a reindex request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html">Reindex API on elastic.co</a>
     * @param reindexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final BulkByScrollResponse reindex(ReindexRequest reindexRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(
            reindexRequest, RequestConverters::reindex, options, BulkByScrollResponse::fromXContent, emptySet()
        );
    }

    /**
     * Asynchronously executes a reindex request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html">Reindex API on elastic.co</a>
     * @param reindexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void reindexAsync(ReindexRequest reindexRequest, RequestOptions options, ActionListener<BulkByScrollResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(
            reindexRequest, RequestConverters::reindex, options, BulkByScrollResponse::fromXContent, listener, emptySet()
        );
    }

    /**
     * Executes a update by query request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html">
     *     Update By Query API on elastic.co</a>
     * @param updateByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final BulkByScrollResponse updateByQuery(UpdateByQueryRequest updateByQueryRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(
            updateByQueryRequest, RequestConverters::updateByQuery, options, BulkByScrollResponse::fromXContent, emptySet()
        );
    }

    /**
     * Asynchronously executes an update by query request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html">
     *     Update By Query API on elastic.co</a>
     * @param updateByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void updateByQueryAsync(UpdateByQueryRequest updateByQueryRequest, RequestOptions options,
                                   ActionListener<BulkByScrollResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(
                updateByQueryRequest, RequestConverters::updateByQuery, options, BulkByScrollResponse::fromXContent, listener, emptySet()
        );
    }

    /**
     * Executes a delete by query request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html">
     *     Delete By Query API on elastic.co</a>
     * @param deleteByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final BulkByScrollResponse deleteByQuery(DeleteByQueryRequest deleteByQueryRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(
            deleteByQueryRequest, RequestConverters::deleteByQuery, options, BulkByScrollResponse::fromXContent, emptySet()
        );
    }

    /**
     * Asynchronously executes a delete by query request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html">
     *     Delete By Query API on elastic.co</a>
     * @param deleteByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void deleteByQueryAsync(DeleteByQueryRequest deleteByQueryRequest, RequestOptions options,
                                         ActionListener<BulkByScrollResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(
                deleteByQueryRequest, RequestConverters::deleteByQuery, options, BulkByScrollResponse::fromXContent, listener, emptySet()
        );
    }

    /**
     * Executes a reindex rethrottling request.
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html#docs-reindex-rethrottle">
     *     Reindex rethrottling API on elastic.co</a>
     * @param rethrottleRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final ListTasksResponse reindexRethrottle(RethrottleRequest rethrottleRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(rethrottleRequest, RequestConverters::rethrottle, options,
            ListTasksResponse::fromXContent, emptySet());
    }

    /**
     * Executes a reindex rethrottling request.
     * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html#docs-reindex-rethrottle">
     *     Reindex rethrottling API on elastic.co</a>
     * @param rethrottleRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void reindexRethrottleAsync(RethrottleRequest rethrottleRequest, RequestOptions options,
            ActionListener<ListTasksResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(rethrottleRequest, RequestConverters::rethrottle, options,
            ListTasksResponse::fromXContent, listener, emptySet());
    }

    /**
     * Pings the remote Elasticsearch cluster and returns true if the ping succeeded, false otherwise
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return <code>true</code> if the ping succeeded, false otherwise
     * @throws IOException in case there is a problem sending the request
     */
    public final boolean ping(RequestOptions options) throws IOException {
        return coreClient.performRequest(new MainRequest(), (request) -> RequestConverters.ping(), options,
            CoreClient::convertExistsResponse, emptySet());
    }

    /**
     * Get the cluster info otherwise provided when sending an HTTP request to '/'
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final MainResponse info(RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(new MainRequest(), (request) -> RequestConverters.info(), options,
                MainResponse::fromXContent, emptySet());
    }

    /**
     * Retrieves a document by id using the Get API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html">Get API on elastic.co</a>
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final GetResponse get(GetRequest getRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(getRequest, RequestConverters::get, options, GetResponse::fromXContent,
            singleton(404));
    }

    /**
     * Asynchronously retrieves a document by id using the Get API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html">Get API on elastic.co</a>
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void getAsync(GetRequest getRequest, RequestOptions options, ActionListener<GetResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(getRequest, RequestConverters::get, options, GetResponse::fromXContent, listener,
                singleton(404));
    }

    /**
     * Retrieves multiple documents by id using the Multi Get API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html">Multi Get API on elastic.co</a>
     * @param multiGetRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #mget(MultiGetRequest, RequestOptions)} instead
     */
    @Deprecated
    public final MultiGetResponse multiGet(MultiGetRequest multiGetRequest, RequestOptions options) throws IOException {
        return mget(multiGetRequest, options);
    }


    /**
     * Retrieves multiple documents by id using the Multi Get API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html">Multi Get API on elastic.co</a>
     * @param multiGetRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final MultiGetResponse mget(MultiGetRequest multiGetRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(multiGetRequest, RequestConverters::multiGet, options,
            MultiGetResponse::fromXContent, singleton(404));
    }

    /**
     * Asynchronously retrieves multiple documents by id using the Multi Get API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html">Multi Get API on elastic.co</a>
     * @param multiGetRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #mgetAsync(MultiGetRequest, RequestOptions, ActionListener)} instead
     */
    @Deprecated
    public final void multiGetAsync(MultiGetRequest multiGetRequest, RequestOptions options, ActionListener<MultiGetResponse> listener) {
        mgetAsync(multiGetRequest, options, listener);
    }

    /**
     * Asynchronously retrieves multiple documents by id using the Multi Get API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html">Multi Get API on elastic.co</a>
     * @param multiGetRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void mgetAsync(MultiGetRequest multiGetRequest, RequestOptions options, ActionListener<MultiGetResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(multiGetRequest, RequestConverters::multiGet, options, MultiGetResponse::fromXContent,
            listener, singleton(404));
    }

    /**
     * Checks for the existence of a document. Returns true if it exists, false otherwise.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html">Get API on elastic.co</a>
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return <code>true</code> if the document exists, <code>false</code> otherwise
     * @throws IOException in case there is a problem sending the request
     */
    public final boolean exists(GetRequest getRequest, RequestOptions options) throws IOException {
        return coreClient.performRequest(getRequest, RequestConverters::exists, options, CoreClient::convertExistsResponse, emptySet());
    }

    /**
     * Asynchronously checks for the existence of a document. Returns true if it exists, false otherwise.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html">Get API on elastic.co</a>
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void existsAsync(GetRequest getRequest, RequestOptions options, ActionListener<Boolean> listener) {
        coreClient.performRequestAsync(getRequest, RequestConverters::exists, options, CoreClient::convertExistsResponse, listener,
                emptySet());
    }

    /**
     * Index a document using the Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html">Index API on elastic.co</a>
     * @param indexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final IndexResponse index(IndexRequest indexRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(indexRequest, RequestConverters::index, options, IndexResponse::fromXContent,
            emptySet());
    }

    /**
     * Asynchronously index a document using the Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html">Index API on elastic.co</a>
     * @param indexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void indexAsync(IndexRequest indexRequest, RequestOptions options, ActionListener<IndexResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(indexRequest, RequestConverters::index, options, IndexResponse::fromXContent, listener,
                emptySet());
    }

    /**
     * Updates a document using the Update API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html">Update API on elastic.co</a>
     * @param updateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final UpdateResponse update(UpdateRequest updateRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(updateRequest, RequestConverters::update, options, UpdateResponse::fromXContent,
            emptySet());
    }

    /**
     * Asynchronously updates a document using the Update API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html">Update API on elastic.co</a>
     * @param updateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void updateAsync(UpdateRequest updateRequest, RequestOptions options, ActionListener<UpdateResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(updateRequest, RequestConverters::update, options, UpdateResponse::fromXContent,
            listener, emptySet());
    }

    /**
     * Deletes a document by id using the Delete API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html">Delete API on elastic.co</a>
     * @param deleteRequest the reuqest
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(deleteRequest, RequestConverters::delete, options, DeleteResponse::fromXContent,
                singleton(404));
    }

    /**
     * Asynchronously deletes a document by id using the Delete API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html">Delete API on elastic.co</a>
     * @param deleteRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void deleteAsync(DeleteRequest deleteRequest, RequestOptions options, ActionListener<DeleteResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(deleteRequest, RequestConverters::delete, options, DeleteResponse::fromXContent,
            listener, Collections.singleton(404));
    }

    /**
     * Executes a search request using the Search API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html">Search API on elastic.co</a>
     * @param searchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final SearchResponse search(SearchRequest searchRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(searchRequest, RequestConverters::search, options, SearchResponse::fromXContent,
            emptySet());
    }

    /**
     * Asynchronously executes a search using the Search API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html">Search API on elastic.co</a>
     * @param searchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void searchAsync(SearchRequest searchRequest, RequestOptions options, ActionListener<SearchResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(searchRequest, RequestConverters::search, options, SearchResponse::fromXContent,
            listener, emptySet());
    }

    /**
     * Executes a multi search using the msearch API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html">Multi search API on
     * elastic.co</a>
     * @param multiSearchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #msearch(MultiSearchRequest, RequestOptions)} instead
     */
    @Deprecated
    public final MultiSearchResponse multiSearch(MultiSearchRequest multiSearchRequest, RequestOptions options) throws IOException {
        return msearch(multiSearchRequest, options);
    }

    /**
     * Executes a multi search using the msearch API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html">Multi search API on
     * elastic.co</a>
     * @param multiSearchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final MultiSearchResponse msearch(MultiSearchRequest multiSearchRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(multiSearchRequest, RequestConverters::multiSearch, options,
            MultiSearchResponse::fromXContext, emptySet());
    }

    /**
     * Asynchronously executes a multi search using the msearch API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html">Multi search API on
     * elastic.co</a>
     * @param searchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #msearchAsync(MultiSearchRequest, RequestOptions, ActionListener)} instead
     */
    @Deprecated
    public final void multiSearchAsync(MultiSearchRequest searchRequest, RequestOptions options,
                                   ActionListener<MultiSearchResponse> listener) {
        msearchAsync(searchRequest, options, listener);
    }

    /**
     * Asynchronously executes a multi search using the msearch API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html">Multi search API on
     * elastic.co</a>
     * @param searchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void msearchAsync(MultiSearchRequest searchRequest, RequestOptions options,
                                   ActionListener<MultiSearchResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(searchRequest, RequestConverters::multiSearch, options,
            MultiSearchResponse::fromXContext, listener, emptySet());
    }

    /**
     * Executes a search using the Search Scroll API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Search Scroll
     * API on elastic.co</a>
     * @param searchScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #scroll(SearchScrollRequest, RequestOptions)} instead
     */
    @Deprecated
    public final SearchResponse searchScroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException {
        return scroll(searchScrollRequest, options);
    }

    /**
     * Executes a search using the Search Scroll API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Search Scroll
     * API on elastic.co</a>
     * @param searchScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final SearchResponse scroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(searchScrollRequest, RequestConverters::searchScroll, options,
            SearchResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously executes a search using the Search Scroll API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Search Scroll
     * API on elastic.co</a>
     * @param searchScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #scrollAsync(SearchScrollRequest, RequestOptions, ActionListener)} instead
     */
    @Deprecated
    public final void searchScrollAsync(SearchScrollRequest searchScrollRequest, RequestOptions options,
                                  ActionListener<SearchResponse> listener) {
        scrollAsync(searchScrollRequest, options, listener);
    }

    /**
     * Asynchronously executes a search using the Search Scroll API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Search Scroll
     * API on elastic.co</a>
     * @param searchScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void scrollAsync(SearchScrollRequest searchScrollRequest, RequestOptions options,
                                  ActionListener<SearchResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(searchScrollRequest, RequestConverters::searchScroll, options,
            SearchResponse::fromXContent, listener, emptySet());
    }

    /**
     * Clears one or more scroll ids using the Clear Scroll API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#_clear_scroll_api">
     * Clear Scroll API on elastic.co</a>
     * @param clearScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(clearScrollRequest, RequestConverters::clearScroll, options,
            ClearScrollResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously clears one or more scroll ids using the Clear Scroll API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#_clear_scroll_api">
     * Clear Scroll API on elastic.co</a>
     * @param clearScrollRequest the reuqest
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void clearScrollAsync(ClearScrollRequest clearScrollRequest, RequestOptions options,
                                       ActionListener<ClearScrollResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(clearScrollRequest, RequestConverters::clearScroll, options,
            ClearScrollResponse::fromXContent, listener, emptySet());
    }

    /**
     * Executes a request using the Search Template API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-template.html">Search Template API
     * on elastic.co</a>.
     * @param searchTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final SearchTemplateResponse searchTemplate(SearchTemplateRequest searchTemplateRequest,
                                                       RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(searchTemplateRequest, RequestConverters::searchTemplate, options,
            SearchTemplateResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously executes a request using the Search Template API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-template.html">Search Template API
     * on elastic.co</a>.
     */
    public final void searchTemplateAsync(SearchTemplateRequest searchTemplateRequest, RequestOptions options,
                                          ActionListener<SearchTemplateResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(searchTemplateRequest, RequestConverters::searchTemplate, options,
            SearchTemplateResponse::fromXContent, listener, emptySet());
    }

    /**
     * Executes a request using the Explain API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-explain.html">Explain API on elastic.co</a>
     * @param explainRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final ExplainResponse explain(ExplainRequest explainRequest, RequestOptions options) throws IOException {
        return coreClient.performRequest(explainRequest, RequestConverters::explain, options,
            response -> {
                CheckedFunction<XContentParser, ExplainResponse, IOException> entityParser =
                    parser -> ExplainResponse.fromXContent(parser, CoreClient.convertExistsResponse(response));
                return coreClient.parseEntity(response.getEntity(), entityParser);
            },
            singleton(404));
    }

    /**
     * Asynchronously executes a request using the Explain API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-explain.html">Explain API on elastic.co</a>
     * @param explainRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void explainAsync(ExplainRequest explainRequest, RequestOptions options, ActionListener<ExplainResponse> listener) {
        coreClient.performRequestAsync(explainRequest, RequestConverters::explain, options,
            response -> {
                CheckedFunction<XContentParser, ExplainResponse, IOException> entityParser =
                    parser -> ExplainResponse.fromXContent(parser, CoreClient.convertExistsResponse(response));
                return coreClient.parseEntity(response.getEntity(), entityParser);
            },
            listener, singleton(404));
    }

    /**
     * Executes a request using the Ranking Evaluation API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-rank-eval.html">Ranking Evaluation API
     * on elastic.co</a>
     * @param rankEvalRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final RankEvalResponse rankEval(RankEvalRequest rankEvalRequest, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(rankEvalRequest, RequestConverters::rankEval, options,
            RankEvalResponse::fromXContent, emptySet());
    }


    /**
     * Executes a request using the Multi Search Template API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-search-template.html">Multi Search Template API
     * on elastic.co</a>.
     */
    public final MultiSearchTemplateResponse msearchTemplate(MultiSearchTemplateRequest multiSearchTemplateRequest,
                                                             RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(multiSearchTemplateRequest, RequestConverters::multiSearchTemplate,
                options, MultiSearchTemplateResponse::fromXContext, emptySet());
    }

    /**
     * Asynchronously executes a request using the Multi Search Template API
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-search-template.html">Multi Search Template API
     * on elastic.co</a>.
     */
    public final void msearchTemplateAsync(MultiSearchTemplateRequest multiSearchTemplateRequest,
                                           RequestOptions options,
                                           ActionListener<MultiSearchTemplateResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(multiSearchTemplateRequest, RequestConverters::multiSearchTemplate,
            options, MultiSearchTemplateResponse::fromXContext, listener, emptySet());
    }

    /**
     * Asynchronously executes a request using the Ranking Evaluation API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-rank-eval.html">Ranking Evaluation API
     * on elastic.co</a>
     * @param rankEvalRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void rankEvalAsync(RankEvalRequest rankEvalRequest, RequestOptions options,  ActionListener<RankEvalResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(rankEvalRequest, RequestConverters::rankEval, options,  RankEvalResponse::fromXContent,
            listener, emptySet());
    }

    /**
     * Executes a request using the Field Capabilities API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-field-caps.html">Field Capabilities API
     * on elastic.co</a>.
     * @param fieldCapabilitiesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public final FieldCapabilitiesResponse fieldCaps(FieldCapabilitiesRequest fieldCapabilitiesRequest,
                                                     RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(fieldCapabilitiesRequest, RequestConverters::fieldCaps, options,
            FieldCapabilitiesResponse::fromXContent, emptySet());
    }

    /**
     * Get stored script by id.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html">
     *     How to use scripts on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetStoredScriptResponse getScript(GetStoredScriptRequest request, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(request, RequestConverters::getScript, options,
            GetStoredScriptResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get stored script by id.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html">
     *     How to use scripts on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void getScriptAsync(GetStoredScriptRequest request, RequestOptions options,
                               ActionListener<GetStoredScriptResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(request, RequestConverters::getScript, options,
            GetStoredScriptResponse::fromXContent, listener, emptySet());
    }

    /**
     * Delete stored script by id.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html">
     *     How to use scripts on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteScript(DeleteStoredScriptRequest request, RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(request, RequestConverters::deleteScript, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete stored script by id.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html">
     *     How to use scripts on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void deleteScriptAsync(DeleteStoredScriptRequest request, RequestOptions options,
                                  ActionListener<AcknowledgedResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(request, RequestConverters::deleteScript, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Puts an stored script using the Scripting API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html"> Scripting API
     * on elastic.co</a>
     * @param putStoredScriptRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putScript(PutStoredScriptRequest putStoredScriptRequest,
                                             RequestOptions options) throws IOException {
        return coreClient.performRequestAndParseEntity(putStoredScriptRequest, RequestConverters::putScript, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously puts an stored script using the Scripting API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html"> Scripting API
     * on elastic.co</a>
     * @param putStoredScriptRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putScriptAsync(PutStoredScriptRequest putStoredScriptRequest, RequestOptions options,
                               ActionListener<AcknowledgedResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(putStoredScriptRequest, RequestConverters::putScript, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously executes a request using the Field Capabilities API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-field-caps.html">Field Capabilities API
     * on elastic.co</a>.
     * @param fieldCapabilitiesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public final void fieldCapsAsync(FieldCapabilitiesRequest fieldCapabilitiesRequest, RequestOptions options,
                                     ActionListener<FieldCapabilitiesResponse> listener) {
        coreClient.performRequestAsyncAndParseEntity(fieldCapabilitiesRequest, RequestConverters::fieldCaps, options,
            FieldCapabilitiesResponse::fromXContent, listener, emptySet());
    }
}
