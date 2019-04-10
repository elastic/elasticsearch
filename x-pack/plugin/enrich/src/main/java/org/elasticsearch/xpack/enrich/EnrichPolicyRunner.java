package org.elasticsearch.xpack.enrich;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class EnrichPolicyRunner {

    private ClusterService clusterService;
    private Client client;
    private LongSupplier nowSupplier;

    public EnrichPolicyRunner(ClusterService clusterService, Client client, LongSupplier nowSupplier) {
        this.clusterService = clusterService;
        this.client = client;
        this.nowSupplier = nowSupplier;
    }

    public void runPolicy(String policyId, ActionListener<EnrichPolicyResult> listener) {
        // TODO once policy store is merged.
        // Look up policy in policy store and execute it
    }

    public void runPolicy(final EnrichPolicy policy, final ActionListener<EnrichPolicyResult> listener) throws Exception {
        // Collect the source index information
        client.admin().indices().getIndex(new GetIndexRequest(), new ActionListener<GetIndexResponse>() {
            @Override
            public void onResponse(GetIndexResponse sourceIndex) {
                validateMappings(sourceIndex, policy, listener);
                prepareAndCreateEnrichIndex(sourceIndex, policy, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void validateMappings(GetIndexResponse getIndexResponse, EnrichPolicy policy, ActionListener<?> listener) {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.mappings();
        // Assuming single mapping
        ImmutableOpenMap<String, MappingMetaData> mapping = mappings.iterator().next().value;
        if (mapping.containsKey(policy.getLookupField()) == false) {
            listener.onFailure(
                new ElasticsearchException("Could not locate primary lookup field [{}] on source mapping for index [{}]",
                    policy.getLookupField(), policy.getSource()));
        }
        for (String enrichField : policy.getEnrichFields()) {
            if (mapping.containsKey(enrichField) == false) {
                listener.onFailure(new ElasticsearchException("Could not locate enrichment field [{}] on source mapping for index [{}]",
                    policy.getLookupField(), policy.getSource()));
            }
        }
    }

    private String getEnrichIndexBase(EnrichPolicy policy) {
        return ".enrich-" + policy.getId();
    }

    private void prepareAndCreateEnrichIndex(GetIndexResponse getIndexResponse, EnrichPolicy policy,
                                             ActionListener<EnrichPolicyResult> listener) {
        long nowTimestamp = nowSupplier.getAsLong();
        String enrichIndexName = getEnrichIndexBase(policy) + "-" + nowTimestamp;
        Settings enrichIndexSettings = Settings.EMPTY;
        Map<String, Object> enrichMapping = resolveEnrichMapping(getIndexResponse, policy);
        CreateIndexRequest createEnrichIndexRequest = new CreateIndexRequest(enrichIndexName, enrichIndexSettings);
        createEnrichIndexRequest.mapping("_doc", enrichMapping);
        client.admin().indices().create(createEnrichIndexRequest, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                transferDataToEnrichIndex(enrichIndexName, enrichMapping.keySet(), policy, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private Map<String, Object> resolveEnrichMapping(GetIndexResponse getIndexResponse, EnrichPolicy policy) {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> sourceMappings = getIndexResponse.mappings();
        ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> typeAndMapping = sourceMappings.iterator().next();
        ImmutableOpenMap<String, MappingMetaData> sourceMapping = typeAndMapping.value;

        Map<String, Object> destinationMapping = new HashMap<>();
        destinationMapping.put(policy.getLookupField(), sourceMapping.get(policy.getLookupField()).sourceAsMap());
        for (String enrichField : policy.getEnrichFields()) {
            destinationMapping.put(enrichField, sourceMapping.get(enrichField).sourceAsMap());
        }
        return destinationMapping;
    }

    private void transferDataToEnrichIndex(String destinationIndexName, Set<String> includeFields, EnrichPolicy policy,
                                           ActionListener<EnrichPolicyResult> listener) {
        // Reindex from source to enrich
        ReindexRequest reindexRequest = new ReindexRequest()
            .setDestIndex(destinationIndexName)
            .setSourceIndices(policy.getSource());
        // Filter down the source fields to just the ones required by the policy
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(includeFields.toArray(new String[0]), new String[0]);
        // TODO: Eventually add in any filtering or query from the policy here
        reindexRequest.getSearchRequest().source(searchSourceBuilder);
        client.execute(ReindexAction.INSTANCE, reindexRequest, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                // Do we want to fail the request if there were failures during the reindex process?
                if (bulkByScrollResponse.getBulkFailures().size() > 0) {
                    listener.onFailure(new ElasticsearchException("Encountered bulk failures during reindex process"));
                } else if (bulkByScrollResponse.getSearchFailures().size() > 0) {
                    listener.onFailure(new ElasticsearchException("Encountered search failures during reindex process"));
                } else {
                    updateEnrichPolicyAlias(destinationIndexName, policy, listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void updateEnrichPolicyAlias(String destinationIndexName, EnrichPolicy policy, ActionListener<EnrichPolicyResult> listener) {
        String enrichIndexBase = getEnrichIndexBase(policy);
        ImmutableOpenMap<String, List<AliasMetaData>> aliases =
            clusterService.state().metaData().findAliases(new GetAliasesRequest(enrichIndexBase), new String[]{enrichIndexBase + "-*"});
        IndicesAliasesRequest aliasToggleRequest = new IndicesAliasesRequest();
        String[] indices = aliases.keys().toArray(String.class);
        aliasToggleRequest
            .addAliasAction(IndicesAliasesRequest.AliasActions.remove().indices(indices).alias(enrichIndexBase))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(destinationIndexName).alias(enrichIndexBase));
        client.admin().indices().aliases(aliasToggleRequest, new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                listener.onResponse(new EnrichPolicyResult(true));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
