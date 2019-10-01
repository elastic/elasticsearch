/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.security.ScrollHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalState;

/**
 * Helper methods for access and storage of an enrich policy.
 */
public final class EnrichStore {
    public static final String ENRICH_INDEX = ".enrich";

    private EnrichStore() {}

    /**
     * Adds a new enrich policy or overwrites an existing policy if there is already a policy with the same name.
     * This method can only be invoked on the elected master node.
     *
     * @param name      The unique name of the policy
     * @param policy    The policy to store
     * @param handler   The handler that gets invoked if policy has been stored or a failure has occurred.
     */
    public static void putPolicy(String name, EnrichPolicy policy, ClusterService clusterService, Client client,
                                 Consumer<Exception> handler) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy is missing");
        }
        // The policy name is used to create the enrich index name and
        // therefor a policy name has the same restrictions as an index name
        MetaDataCreateIndexService.validateIndexOrAliasName(name,
            (policyName, error) -> new IllegalArgumentException("Invalid policy name [" + policyName + "], " + error));
        if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
            throw new IllegalArgumentException("Invalid policy name [" + name + "], must be lowercase");
        }
        // TODO: add policy validation

        final EnrichPolicy finalPolicy;
        if (policy.getElasticsearchVersion() == null) {
            finalPolicy = new EnrichPolicy(
                policy.getType(),
                policy.getQuery(),
                policy.getIndices(),
                policy.getMatchField(),
                policy.getEnrichFields(),
                Version.CURRENT
            );
        } else {
            finalPolicy = policy;
        }

        ensureIndex(clusterService.state(), client, val -> {
            if (val == null) {
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    finalPolicy.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    client.index(new IndexRequest(ENRICH_INDEX)
                        .id(name)
                        // ensure it cannot be overwritten
                        .opType(DocWriteRequest.OpType.CREATE)
                        .source(builder), new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            if (indexResponse.getResult() != DocWriteResponse.Result.CREATED) {
                                handler.accept(new ElasticsearchException("policy [{}] was not created", name));
                            } else {
                                handler.accept(null);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof VersionConflictEngineException) {
                                // wrap to a nicer exception that makes sense to the user
                                throw new ResourceAlreadyExistsException("policy [{}] already exists", name);
                            } else {
                                handler.accept(e);
                            }
                        }
                    });
                } catch (Exception e) {
                    handler.accept(e);
                }
            } else {
                handler.accept(val);
            }
        });
    }

    /**
     * Removes an enrich policy from the policies in the cluster state. This method can only be invoked on the
     * elected master node.
     *
     * @param name      The unique name of the policy
     * @param handler   The handler that gets invoked if policy has been stored or a failure has occurred.
     */
    public static void deletePolicy(String name, ClusterService clusterService, Client client, Consumer<Exception> handler) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        ensureIndex(clusterService.state(), client, val -> {
            if (val == null) {
                client.delete(new DeleteRequest(ENRICH_INDEX, name), new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                            handler.accept(new ResourceNotFoundException("policy [{}] not found", name));
                        } else {
                            handler.accept(null);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        handler.accept(e);
                    }
                });
            } else {
                handler.accept(val);
            }
        });
    }

    /**
     * Gets an enrich policy for the provided name if exists or otherwise returns <code>null</code>.
     *
     * @param name  The name of the policy to fetch
     * @param listener returns an enrich policy if exists or <code>null</code> otherwise
     */
    public static void getPolicy(String name, ClusterState state, Client client,
                                         ActionListener<EnrichPolicy.NamedPolicy> listener) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        getPolicies(state, client, ActionListener.wrap(
            policies -> {
                listener.onResponse(policies.stream().filter(p -> p.getName().equals(name)).findFirst().orElse(null));
            },
            listener::onFailure
        ));
    }

    /**
     * Gets all policies in the cluster.
     *
     * @param state the cluster state
     * @param listener returns a Collection of NamedPolicies
     */
    public static void getPolicies(ClusterState state, Client client, ActionListener<Collection<EnrichPolicy.NamedPolicy>> listener) {
        final AliasOrIndex enrichIndex = state.metaData().getAliasAndIndexLookup().get(ENRICH_INDEX);
        if (enrichIndex != null && enrichIndex.isAlias()) {
            throw new IllegalStateException("The enrich index exists and is an alias");
        }

        // if the index exists, refresh before searching
        if (enrichIndex != null) {
            if (enrichIndex.getIndices().isEmpty()) {
                throw new IllegalStateException("the enrich index has no index metadata");
            }

            final ActionListener<RefreshResponse> refreshAndGetPoliciesListener = ActionListener.wrap(
                response -> {
                    if (response.getSuccessfulShards() < enrichIndex.getIndices().get(0).getNumberOfShards()) {
                        throw illegalState("not all required shards have been refreshed");
                    }
                    innerGetPolicies(state, client, listener);
                },
                listener::onFailure
            );
            client.admin().indices().refresh(new RefreshRequest(ENRICH_INDEX), refreshAndGetPoliciesListener);
        } else {
            // return an empty list since the index does not exist
            listener.onResponse(List.of());
        }
    }

    private static void innerGetPolicies(ClusterState state, Client client, ActionListener<Collection<EnrichPolicy.NamedPolicy>> listener) {
        ensureIndex(state, client, val -> {
            if (val == null) {
                ScrollHelper.fetchAllByEntity(client, new SearchRequest(ENRICH_INDEX).scroll(TimeValue.timeValueSeconds(3)), listener,
                    hit -> {
                        try {
                            XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, null, hit.getSourceRef(),
                                XContentType.JSON);
                            return new EnrichPolicy.NamedPolicy(hit.getId(), EnrichPolicy.fromXContent(parser));
                        } catch (IOException e) {
                            throw new ElasticsearchParseException("Could not parse enrich policy from hit [{}]", hit.getId(), e);
                        }
                    }
                );
            } else {
                listener.onFailure(val);
            }
        });
    }

    private static void ensureIndex(ClusterState state, Client client, Consumer<Exception> handler) {
        final AliasOrIndex enrichIndex = state.metaData().getAliasAndIndexLookup().get(ENRICH_INDEX);
        if (enrichIndex != null && enrichIndex.isAlias()) {
            throw new IllegalStateException("The enrich index exists and is an alias");
        }

        if (enrichIndex == null) {
            // create the index
            client.admin().indices().prepareCreate(ENRICH_INDEX)
                .setWaitForActiveShards(1)
                .execute(new ActionListener<CreateIndexResponse>() {
                    @Override
                    public void onResponse(CreateIndexResponse createIndexResponse) {
                        if (createIndexResponse.isAcknowledged() == false) {
                            handler.accept(new ElasticsearchException("policy index was not created"));
                        } else {
                            handler.accept(null);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof ResourceAlreadyExistsException) {
                            // assume that there is a race between the index was checked and created,
                            // so this is likely multiple calls were issued to ensureIndex.
                            handler.accept(null);
                        } else {
                            handler.accept(e);
                        }
                    }
                });
        } else {
            handler.accept(null);
        }
    }
}
