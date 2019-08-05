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

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.FreezeIndexRequest;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.indices.ReloadAnalyzersRequest;
import org.elasticsearch.client.indices.UnfreezeIndexRequest;
import org.elasticsearch.client.indices.rollover.RolloverRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;

final class IndicesRequestConverters {

    private IndicesRequestConverters() {}

    static Request deleteIndex(DeleteIndexRequest deleteIndexRequest) {
        String endpoint = RequestConverters.endpoint(deleteIndexRequest.indices());
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(deleteIndexRequest.timeout());
        parameters.withMasterTimeout(deleteIndexRequest.masterNodeTimeout());
        parameters.withIndicesOptions(deleteIndexRequest.indicesOptions());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request openIndex(OpenIndexRequest openIndexRequest) {
        String endpoint = RequestConverters.endpoint(openIndexRequest.indices(), "_open");
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(openIndexRequest.timeout());
        parameters.withMasterTimeout(openIndexRequest.masterNodeTimeout());
        parameters.withWaitForActiveShards(openIndexRequest.waitForActiveShards());
        parameters.withIndicesOptions(openIndexRequest.indicesOptions());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request closeIndex(CloseIndexRequest closeIndexRequest) {
        String endpoint = RequestConverters.endpoint(closeIndexRequest.indices(), "_close");
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(closeIndexRequest.timeout());
        parameters.withMasterTimeout(closeIndexRequest.masterNodeTimeout());
        parameters.withIndicesOptions(closeIndexRequest.indicesOptions());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request createIndex(CreateIndexRequest createIndexRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPart(createIndexRequest.index()).build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(createIndexRequest.timeout());
        parameters.withMasterTimeout(createIndexRequest.masterNodeTimeout());
        parameters.withWaitForActiveShards(createIndexRequest.waitForActiveShards());
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(createIndexRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request createIndex(org.elasticsearch.action.admin.indices.create.CreateIndexRequest createIndexRequest)
            throws IOException {
        String endpoint = RequestConverters.endpoint(createIndexRequest.indices());
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(createIndexRequest.timeout());
        parameters.withMasterTimeout(createIndexRequest.masterNodeTimeout());
        parameters.withWaitForActiveShards(createIndexRequest.waitForActiveShards());
        parameters.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(createIndexRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request updateAliases(IndicesAliasesRequest indicesAliasesRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_aliases");

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(indicesAliasesRequest.timeout());
        parameters.withMasterTimeout(indicesAliasesRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(indicesAliasesRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }


    static Request putMapping(PutMappingRequest putMappingRequest) throws IOException {
        Request request = new Request(HttpPut.METHOD_NAME, RequestConverters.endpoint(putMappingRequest.indices(), "_mapping"));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(putMappingRequest.timeout());
        parameters.withMasterTimeout(putMappingRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(putMappingRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    /**
     * converter for the legacy server-side {@link org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest} that still supports
     * types
     */
    @Deprecated
    static Request putMapping(org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest putMappingRequest) throws IOException {
        // The concreteIndex is an internal concept, not applicable to requests made over the REST API.
        if (putMappingRequest.getConcreteIndex() != null) {
            throw new IllegalArgumentException("concreteIndex cannot be set on PutMapping requests made over the REST API");
        }

        Request request = new Request(HttpPut.METHOD_NAME, RequestConverters.endpoint(putMappingRequest.indices(),
            "_mapping", putMappingRequest.type()));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(putMappingRequest.timeout());
        parameters.withMasterTimeout(putMappingRequest.masterNodeTimeout());
        parameters.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(putMappingRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getMappings(GetMappingsRequest getMappingsRequest) {
        String[] indices = getMappingsRequest.indices() == null ? Strings.EMPTY_ARRAY : getMappingsRequest.indices();

        Request request = new Request(HttpGet.METHOD_NAME, RequestConverters.endpoint(indices, "_mapping"));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(getMappingsRequest.masterNodeTimeout());
        parameters.withIndicesOptions(getMappingsRequest.indicesOptions());
        parameters.withLocal(getMappingsRequest.local());
        request.addParameters(parameters.asMap());
        return request;
    }

    @Deprecated
    static Request getMappings(org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest getMappingsRequest) {
        String[] indices = getMappingsRequest.indices() == null ? Strings.EMPTY_ARRAY : getMappingsRequest.indices();
        String[] types = getMappingsRequest.types() == null ? Strings.EMPTY_ARRAY : getMappingsRequest.types();

        Request request = new Request(HttpGet.METHOD_NAME, RequestConverters.endpoint(indices, "_mapping", types));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(getMappingsRequest.masterNodeTimeout());
        parameters.withIndicesOptions(getMappingsRequest.indicesOptions());
        parameters.withLocal(getMappingsRequest.local());
        parameters.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request getFieldMapping(GetFieldMappingsRequest getFieldMappingsRequest) {
        String[] indices = getFieldMappingsRequest.indices() == null ? Strings.EMPTY_ARRAY : getFieldMappingsRequest.indices();
        String[] fields = getFieldMappingsRequest.fields() == null ? Strings.EMPTY_ARRAY : getFieldMappingsRequest.fields();

        String endpoint = new RequestConverters.EndpointBuilder()
            .addCommaSeparatedPathParts(indices)
            .addPathPartAsIs("_mapping")
            .addPathPartAsIs("field")
            .addCommaSeparatedPathParts(fields)
            .build();

        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(getFieldMappingsRequest.indicesOptions());
        parameters.withIncludeDefaults(getFieldMappingsRequest.includeDefaults());
        parameters.withLocal(getFieldMappingsRequest.local());
        request.addParameters(parameters.asMap());
        return request;
    }

    @Deprecated
    static Request getFieldMapping(org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest getFieldMappingsRequest) {
        String[] indices = getFieldMappingsRequest.indices() == null ? Strings.EMPTY_ARRAY : getFieldMappingsRequest.indices();
        String[] types = getFieldMappingsRequest.types() == null ? Strings.EMPTY_ARRAY : getFieldMappingsRequest.types();
        String[] fields = getFieldMappingsRequest.fields() == null ? Strings.EMPTY_ARRAY : getFieldMappingsRequest.fields();

        String endpoint = new RequestConverters.EndpointBuilder().addCommaSeparatedPathParts(indices)
            .addPathPartAsIs("_mapping").addCommaSeparatedPathParts(types)
            .addPathPartAsIs("field").addCommaSeparatedPathParts(fields)
            .build();

        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(getFieldMappingsRequest.indicesOptions());
        parameters.withIncludeDefaults(getFieldMappingsRequest.includeDefaults());
        parameters.withLocal(getFieldMappingsRequest.local());
        parameters.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request refresh(RefreshRequest refreshRequest) {
        String[] indices = refreshRequest.indices() == null ? Strings.EMPTY_ARRAY : refreshRequest.indices();
        Request request = new Request(HttpPost.METHOD_NAME, RequestConverters.endpoint(indices, "_refresh"));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(refreshRequest.indicesOptions());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request flush(FlushRequest flushRequest) {
        String[] indices = flushRequest.indices() == null ? Strings.EMPTY_ARRAY : flushRequest.indices();
        Request request = new Request(HttpPost.METHOD_NAME, RequestConverters.endpoint(indices, "_flush"));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(flushRequest.indicesOptions());
        parameters.putParam("wait_if_ongoing", Boolean.toString(flushRequest.waitIfOngoing()));
        parameters.putParam("force", Boolean.toString(flushRequest.force()));
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request flushSynced(SyncedFlushRequest syncedFlushRequest) {
        String[] indices = syncedFlushRequest.indices() == null ? Strings.EMPTY_ARRAY : syncedFlushRequest.indices();
        Request request = new Request(HttpPost.METHOD_NAME, RequestConverters.endpoint(indices, "_flush/synced"));
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(syncedFlushRequest.indicesOptions());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request forceMerge(ForceMergeRequest forceMergeRequest) {
        String[] indices = forceMergeRequest.indices() == null ? Strings.EMPTY_ARRAY : forceMergeRequest.indices();
        Request request = new Request(HttpPost.METHOD_NAME, RequestConverters.endpoint(indices, "_forcemerge"));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(forceMergeRequest.indicesOptions());
        parameters.putParam("max_num_segments", Integer.toString(forceMergeRequest.maxNumSegments()));
        parameters.putParam("only_expunge_deletes", Boolean.toString(forceMergeRequest.onlyExpungeDeletes()));
        parameters.putParam("flush", Boolean.toString(forceMergeRequest.flush()));
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request clearCache(ClearIndicesCacheRequest clearIndicesCacheRequest) {
        String[] indices = clearIndicesCacheRequest.indices() == null ? Strings.EMPTY_ARRAY :clearIndicesCacheRequest.indices();
        Request request = new Request(HttpPost.METHOD_NAME, RequestConverters.endpoint(indices, "_cache/clear"));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(clearIndicesCacheRequest.indicesOptions());
        parameters.putParam("query", Boolean.toString(clearIndicesCacheRequest.queryCache()));
        parameters.putParam("fielddata", Boolean.toString(clearIndicesCacheRequest.fieldDataCache()));
        parameters.putParam("request", Boolean.toString(clearIndicesCacheRequest.requestCache()));
        parameters.putParam("fields", String.join(",", clearIndicesCacheRequest.fields()));
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request existsAlias(GetAliasesRequest getAliasesRequest) {
        if ((getAliasesRequest.indices() == null || getAliasesRequest.indices().length == 0) &&
                (getAliasesRequest.aliases() == null || getAliasesRequest.aliases().length == 0)) {
            throw new IllegalArgumentException("existsAlias requires at least an alias or an index");
        }
        String[] indices = getAliasesRequest.indices() == null ? Strings.EMPTY_ARRAY : getAliasesRequest.indices();
        String[] aliases = getAliasesRequest.aliases() == null ? Strings.EMPTY_ARRAY : getAliasesRequest.aliases();

        Request request = new Request(HttpHead.METHOD_NAME, RequestConverters.endpoint(indices, "_alias", aliases));

        RequestConverters.Params params = new RequestConverters.Params();
        params.withIndicesOptions(getAliasesRequest.indicesOptions());
        params.withLocal(getAliasesRequest.local());
        request.addParameters(params.asMap());
        return request;
    }

    static Request split(ResizeRequest resizeRequest) throws IOException {
        if (resizeRequest.getResizeType() != ResizeType.SPLIT) {
            throw new IllegalArgumentException("Wrong resize type [" + resizeRequest.getResizeType() + "] for indices split request");
        }
        return resize(resizeRequest);
    }

    static Request shrink(ResizeRequest resizeRequest) throws IOException {
        if (resizeRequest.getResizeType() != ResizeType.SHRINK) {
            throw new IllegalArgumentException("Wrong resize type [" + resizeRequest.getResizeType() + "] for indices shrink request");
        }
        return resize(resizeRequest);
    }

    static Request clone(ResizeRequest resizeRequest) throws IOException {
        if (resizeRequest.getResizeType() != ResizeType.CLONE) {
            throw new IllegalArgumentException("Wrong resize type [" + resizeRequest.getResizeType() + "] for indices clone request");
        }
        return resize(resizeRequest);
    }

    private static Request resize(ResizeRequest resizeRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPart(resizeRequest.getSourceIndex())
                .addPathPartAsIs("_" + resizeRequest.getResizeType().name().toLowerCase(Locale.ROOT))
                .addPathPart(resizeRequest.getTargetIndexRequest().index()).build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withTimeout(resizeRequest.timeout());
        params.withMasterTimeout(resizeRequest.masterNodeTimeout());
        params.withWaitForActiveShards(resizeRequest.getTargetIndexRequest().waitForActiveShards());
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(resizeRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request rollover(RolloverRequest rolloverRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPart(rolloverRequest.getAlias()).addPathPartAsIs("_rollover")
            .addPathPart(rolloverRequest.getNewIndexName()).build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withTimeout(rolloverRequest.timeout());
        params.withMasterTimeout(rolloverRequest.masterNodeTimeout());
        params.withWaitForActiveShards(rolloverRequest.getCreateIndexRequest().waitForActiveShards());
        if (rolloverRequest.isDryRun()) {
            params.putParam("dry_run", Boolean.TRUE.toString());
        }
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(rolloverRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    @Deprecated
    static Request rollover(org.elasticsearch.action.admin.indices.rollover.RolloverRequest rolloverRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPart(rolloverRequest.getAlias()).addPathPartAsIs("_rollover")
            .addPathPart(rolloverRequest.getNewIndexName()).build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withTimeout(rolloverRequest.timeout());
        params.withMasterTimeout(rolloverRequest.masterNodeTimeout());
        params.withWaitForActiveShards(rolloverRequest.getCreateIndexRequest().waitForActiveShards());
        if (rolloverRequest.isDryRun()) {
            params.putParam("dry_run", Boolean.TRUE.toString());
        }
        params.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        request.setEntity(RequestConverters.createEntity(rolloverRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        request.addParameters(params.asMap());
        return request;
    }

    static Request getSettings(GetSettingsRequest getSettingsRequest) {
        String[] indices = getSettingsRequest.indices() == null ? Strings.EMPTY_ARRAY : getSettingsRequest.indices();
        String[] names = getSettingsRequest.names() == null ? Strings.EMPTY_ARRAY : getSettingsRequest.names();

        String endpoint = RequestConverters.endpoint(indices, "_settings", names);
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withIndicesOptions(getSettingsRequest.indicesOptions());
        params.withLocal(getSettingsRequest.local());
        params.withIncludeDefaults(getSettingsRequest.includeDefaults());
        params.withMasterTimeout(getSettingsRequest.masterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    /**
     * converter for the legacy server-side {@link org.elasticsearch.action.admin.indices.get.GetIndexRequest} that
     * still supports types
     */
    @Deprecated
    static Request getIndex(org.elasticsearch.action.admin.indices.get.GetIndexRequest getIndexRequest) {
        String[] indices = getIndexRequest.indices() == null ? Strings.EMPTY_ARRAY : getIndexRequest.indices();

        String endpoint = RequestConverters.endpoint(indices);
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withIndicesOptions(getIndexRequest.indicesOptions());
        params.withLocal(getIndexRequest.local());
        params.withIncludeDefaults(getIndexRequest.includeDefaults());
        params.withHuman(getIndexRequest.humanReadable());
        params.withMasterTimeout(getIndexRequest.masterNodeTimeout());
        params.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        request.addParameters(params.asMap());
        return request;
    }

    static Request getIndex(GetIndexRequest getIndexRequest) {
        String[] indices = getIndexRequest.indices() == null ? Strings.EMPTY_ARRAY : getIndexRequest.indices();

        String endpoint = RequestConverters.endpoint(indices);
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withIndicesOptions(getIndexRequest.indicesOptions());
        params.withLocal(getIndexRequest.local());
        params.withIncludeDefaults(getIndexRequest.includeDefaults());
        params.withHuman(getIndexRequest.humanReadable());
        params.withMasterTimeout(getIndexRequest.masterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    /**
     * converter for the legacy server-side {@link org.elasticsearch.action.admin.indices.get.GetIndexRequest} that
     * still supports types
     */
    @Deprecated
    static Request indicesExist(org.elasticsearch.action.admin.indices.get.GetIndexRequest getIndexRequest) {
        if (getIndexRequest.indices() == null || getIndexRequest.indices().length == 0) {
            throw new IllegalArgumentException("indices are mandatory");
        }
        String endpoint = RequestConverters.endpoint(getIndexRequest.indices(), "");
        Request request = new Request(HttpHead.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withLocal(getIndexRequest.local());
        params.withHuman(getIndexRequest.humanReadable());
        params.withIndicesOptions(getIndexRequest.indicesOptions());
        params.withIncludeDefaults(getIndexRequest.includeDefaults());
        params.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        request.addParameters(params.asMap());
        return request;
    }

    static Request indicesExist(GetIndexRequest getIndexRequest) {
        if (getIndexRequest.indices() == null || getIndexRequest.indices().length == 0) {
            throw new IllegalArgumentException("indices are mandatory");
        }
        String endpoint = RequestConverters.endpoint(getIndexRequest.indices(), "");
        Request request = new Request(HttpHead.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withLocal(getIndexRequest.local());
        params.withHuman(getIndexRequest.humanReadable());
        params.withIndicesOptions(getIndexRequest.indicesOptions());
        params.withIncludeDefaults(getIndexRequest.includeDefaults());
        request.addParameters(params.asMap());
        return request;
    }

    static Request indexPutSettings(UpdateSettingsRequest updateSettingsRequest) throws IOException {
        String[] indices = updateSettingsRequest.indices() == null ? Strings.EMPTY_ARRAY : updateSettingsRequest.indices();
        Request request = new Request(HttpPut.METHOD_NAME, RequestConverters.endpoint(indices, "_settings"));

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(updateSettingsRequest.timeout());
        parameters.withMasterTimeout(updateSettingsRequest.masterNodeTimeout());
        parameters.withIndicesOptions(updateSettingsRequest.indicesOptions());
        parameters.withPreserveExisting(updateSettingsRequest.isPreserveExisting());
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(updateSettingsRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    /**
     * @deprecated This uses the old form of PutIndexTemplateRequest which uses types.
     * Use (@link {@link #putTemplate(PutIndexTemplateRequest)} instead
     */
    @Deprecated
    static Request putTemplate(org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest putIndexTemplateRequest)
            throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_template")
            .addPathPart(putIndexTemplateRequest.name()).build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(putIndexTemplateRequest.masterNodeTimeout());
        if (putIndexTemplateRequest.create()) {
            params.putParam("create", Boolean.TRUE.toString());
        }
        if (Strings.hasText(putIndexTemplateRequest.cause())) {
            params.putParam("cause", putIndexTemplateRequest.cause());
        }
        params.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(putIndexTemplateRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request putTemplate(PutIndexTemplateRequest putIndexTemplateRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_template")
            .addPathPart(putIndexTemplateRequest.name()).build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(putIndexTemplateRequest.masterNodeTimeout());
        if (putIndexTemplateRequest.create()) {
            params.putParam("create", Boolean.TRUE.toString());
        }
        if (Strings.hasText(putIndexTemplateRequest.cause())) {
            params.putParam("cause", putIndexTemplateRequest.cause());
        }
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(putIndexTemplateRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request validateQuery(ValidateQueryRequest validateQueryRequest) throws IOException {
        String[] indices = validateQueryRequest.indices() == null ? Strings.EMPTY_ARRAY : validateQueryRequest.indices();
        String[] types = validateQueryRequest.types() == null || indices.length <= 0 ? Strings.EMPTY_ARRAY : validateQueryRequest.types();
        String endpoint = RequestConverters.endpoint(indices, types, "_validate/query");
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withIndicesOptions(validateQueryRequest.indicesOptions());
        params.putParam("explain", Boolean.toString(validateQueryRequest.explain()));
        params.putParam("all_shards", Boolean.toString(validateQueryRequest.allShards()));
        params.putParam("rewrite", Boolean.toString(validateQueryRequest.rewrite()));
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(validateQueryRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getAlias(GetAliasesRequest getAliasesRequest) {
        String[] indices = getAliasesRequest.indices() == null ? Strings.EMPTY_ARRAY : getAliasesRequest.indices();
        String[] aliases = getAliasesRequest.aliases() == null ? Strings.EMPTY_ARRAY : getAliasesRequest.aliases();
        String endpoint = RequestConverters.endpoint(indices, "_alias", aliases);
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withIndicesOptions(getAliasesRequest.indicesOptions());
        params.withLocal(getAliasesRequest.local());
        request.addParameters(params.asMap());
        return request;
    }

    @Deprecated
    static Request getTemplatesWithDocumentTypes(GetIndexTemplatesRequest getIndexTemplatesRequest) {
        return getTemplates(getIndexTemplatesRequest, true);
    }

    static Request getTemplates(GetIndexTemplatesRequest getIndexTemplatesRequest) {
        return getTemplates(getIndexTemplatesRequest, false);
    }

    private static Request getTemplates(GetIndexTemplatesRequest getIndexTemplatesRequest, boolean includeTypeName) {
        final String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_template")
            .addCommaSeparatedPathParts(getIndexTemplatesRequest.names())
            .build();
        final Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        final RequestConverters.Params params = new RequestConverters.Params();
        params.withLocal(getIndexTemplatesRequest.isLocal());
        params.withMasterTimeout(getIndexTemplatesRequest.getMasterNodeTimeout());
        if (includeTypeName) {
            params.putParam(INCLUDE_TYPE_NAME_PARAMETER, "true");
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request templatesExist(IndexTemplatesExistRequest indexTemplatesExistRequest) {
        final String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_template")
            .addCommaSeparatedPathParts(indexTemplatesExistRequest.names())
            .build();
        final Request request = new Request(HttpHead.METHOD_NAME, endpoint);
        final RequestConverters.Params params = new RequestConverters.Params();
        params.withLocal(indexTemplatesExistRequest.isLocal());
        params.withMasterTimeout(indexTemplatesExistRequest.getMasterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request analyze(AnalyzeRequest request) throws IOException {
        RequestConverters.EndpointBuilder builder = new RequestConverters.EndpointBuilder();
        String index = request.index();
        if (index != null) {
            builder.addPathPart(index);
        }
        builder.addPathPartAsIs("_analyze");
        Request req = new Request(HttpGet.METHOD_NAME, builder.build());
        req.setEntity(RequestConverters.createEntity(request, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return req;
    }

    static Request freezeIndex(FreezeIndexRequest freezeIndexRequest) {
        String endpoint = RequestConverters.endpoint(freezeIndexRequest.getIndices(), "_freeze");
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(freezeIndexRequest.timeout());
        parameters.withMasterTimeout(freezeIndexRequest.masterNodeTimeout());
        parameters.withIndicesOptions(freezeIndexRequest.indicesOptions());
        parameters.withWaitForActiveShards(freezeIndexRequest.getWaitForActiveShards());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request unfreezeIndex(UnfreezeIndexRequest unfreezeIndexRequest) {
        String endpoint = RequestConverters.endpoint(unfreezeIndexRequest.getIndices(), "_unfreeze");
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(unfreezeIndexRequest.timeout());
        parameters.withMasterTimeout(unfreezeIndexRequest.masterNodeTimeout());
        parameters.withIndicesOptions(unfreezeIndexRequest.indicesOptions());
        parameters.withWaitForActiveShards(unfreezeIndexRequest.getWaitForActiveShards());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request deleteTemplate(DeleteIndexTemplateRequest deleteIndexTemplateRequest) {
        String name = deleteIndexTemplateRequest.name();
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_template").addPathPart(name).build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(deleteIndexTemplateRequest.masterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request reloadAnalyzers(ReloadAnalyzersRequest reloadAnalyzersRequest) {
        String endpoint = RequestConverters.endpoint(reloadAnalyzersRequest.getIndices(), "_reload_search_analyzers");
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(reloadAnalyzersRequest.indicesOptions());
        request.addParameters(parameters.asMap());
        return request;
    }
}
