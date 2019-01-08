/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static org.elasticsearch.index.IndexSettings.same;

public class SecurityIndexUpgradeCheck implements UpgradeCheck {

    private static final String SECURITY_ALIAS_NAME = ".security";
    private static final String TEMPLATE_VERSION_PATTERN = Pattern.quote("${security.template.version}");
    private static final int INTERNAL_SECURITY_INDEX_FORMAT = 6;
    private static final String INTERNAL_SECURITY_INDEX = ".security-" + INTERNAL_SECURITY_INDEX_FORMAT;
    private static final String NEW_INTERNAL_SECURITY_INDEX = ".security-" + (INTERNAL_SECURITY_INDEX_FORMAT + 1);
    private static final String NEW_SECURITY_TEMPLATE_NAME = "new-security-index-template";

    private static final String SECURITY_TOKENS_ALIAS_NAME = ".security-tokens";
    private static final int INTERNAL_SECURITY_TOKENS_INDEX_FORMAT = 7;
    private static final String INTERNAL_SECURITY_TOKENS_INDEX = ".security-tokens-" + INTERNAL_SECURITY_TOKENS_INDEX_FORMAT;
    private static final String SECURITY_TOKENS_TEMPLATE_NAME = "security-tokens-index-template";

    private final String name;
    private final Client client;
    private final ClusterService clusterService;
    
    public SecurityIndexUpgradeCheck(String name, Client client, ClusterService clusterService) {
        this.name = name;
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Returns the name of the check
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * This method is called by Upgrade API to verify if upgrade or reindex for this index is required
     *
     * @param indexMetaData index metadata
     * @return required action or UpgradeActionRequired.NOT_APPLICABLE if this check cannot be performed on the index
     */
    @Override
    public UpgradeActionRequired actionRequired(IndexMetaData indexMetaData) {
        if (indexMetaData.getIndex().getName().equals(INTERNAL_SECURITY_INDEX)) {
            // no need to check the "index.format" setting value because the name encodes the format version
            return UpgradeActionRequired.UPGRADE;
        } else if (indexMetaData.getIndex().getName().equals(NEW_INTERNAL_SECURITY_INDEX)) {
            // no need to check the "index.format" setting value because the name encodes the format version
            return UpgradeActionRequired.UP_TO_DATE;
        } else if (indexMetaData.getIndex().getName().equals(INTERNAL_SECURITY_TOKENS_INDEX)) {
            // no need to check the "index.format" setting value because the name encodes the format version
            return UpgradeActionRequired.UP_TO_DATE;
        } else {
            return UpgradeActionRequired.NOT_APPLICABLE;
        }
    }

    /**
     * Perform the index upgrade
     *
     * @param task          the task that executes the upgrade operation
     * @param indexMetaData index metadata
     * @param state         current cluster state
     * @param listener      the listener that should be called upon completion of the upgrade
     */
    @Override
    public void upgrade(TaskId task, IndexMetaData indexMetaData, ClusterState state, ActionListener<BulkByScrollResponse> listener) {
        final ParentTaskAssigningClient parentAwareClient = new ParentTaskAssigningClient(client, task);
        final BoolQueryBuilder tokensQuery = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", "token"));
        final BoolQueryBuilder nonTokensQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("doc_type", "token")));
        final Consumer<Exception> removeReadOnlyBlock = e ->
            removeReadOnlyBlock(parentAwareClient, INTERNAL_SECURITY_INDEX, ActionListener.wrap(unsetReadOnlyResponse -> {
                listener.onFailure(e);
            }, e1 -> {
                e.addSuppressed(e1);
                listener.onFailure(e);
            }));
        // check all nodes can handle the new index format
        checkMasterAndDataNodeVersion(state);
        // create the two new indices, but without their corresponding aliases
        createNewSecurityIndices(parentAwareClient, ActionListener.wrap(createIndicesResponse -> {
            // set a read-only block on the original .security-6 index
            setReadOnlyBlock(INTERNAL_SECURITY_INDEX, ActionListener.wrap(setReadOnlyResponse -> {
                // firstly, reindex all the tokens to the new tokens index
                reindex(parentAwareClient, INTERNAL_SECURITY_INDEX, INTERNAL_SECURITY_TOKENS_INDEX, tokensQuery,
                        ActionListener.wrap(reindexTokensResponse -> {
                            // create the alias pointing to the new tokens index; now the new tokens index is writable anew
                            parentAwareClient.admin().indices().prepareAliases()
                                    .addAlias(INTERNAL_SECURITY_TOKENS_INDEX, SECURITY_TOKENS_ALIAS_NAME)
                                    .execute(ActionListener.wrap(tokensAliasResponse -> {
                                        // now reindex all the other non-token security objects to the new .security-7 index
                                        reindex(parentAwareClient, INTERNAL_SECURITY_INDEX, NEW_INTERNAL_SECURITY_INDEX, nonTokensQuery,
                                                ActionListener.wrap(reindexNonTokensResponse -> {
                                                    // move the .security alias to the new index, and remove the old .security-6 index
                                                    parentAwareClient.admin().indices().prepareAliases()
                                                            .addAlias(NEW_INTERNAL_SECURITY_INDEX, SECURITY_ALIAS_NAME)
                                                            .removeAlias(INTERNAL_SECURITY_INDEX, SECURITY_ALIAS_NAME)
                                                            .removeIndex(INTERNAL_SECURITY_INDEX)
                                                            .execute(ActionListener.wrap(aliasResponse -> {
                                                                // merge both reindex responses to return to the action listener
                                                                listener.onResponse(new BulkByScrollResponse(
                                                                        Arrays.asList(reindexTokensResponse, reindexNonTokensResponse), null));
                                                            }, removeReadOnlyBlock));
                                                }, removeReadOnlyBlock));
                                    }, removeReadOnlyBlock));
                        }, removeReadOnlyBlock));
            }, listener::onFailure));
        }, listener::onFailure));
    }

    private void checkMasterAndDataNodeVersion(ClusterState clusterState) {
        if (clusterState.nodes().getMinNodeVersion().before(Upgrade.UPGRADE_INTRODUCED)) {
            throw new IllegalStateException("All nodes should have at least version [" + Upgrade.UPGRADE_INTRODUCED + "] to upgrade");
        }
    }

    private void createNewSecurityIndices(ParentTaskAssigningClient parentAwareClient, ActionListener<AcknowledgedResponse> listener) {
        final Tuple<String, Settings> tokensMappingAndSettings = loadMappingAndSettingsSourceFromTemplate(SECURITY_TOKENS_TEMPLATE_NAME);
        parentAwareClient.admin().indices().prepareCreate(INTERNAL_SECURITY_TOKENS_INDEX)
            .addMapping("doc", tokensMappingAndSettings.v1(), XContentType.JSON)
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .setSettings(tokensMappingAndSettings.v2())
            .execute(ActionListener.wrap(createTokensIndexResponse -> {
                final Tuple<String, Settings> objMappingAndSettings = loadMappingAndSettingsSourceFromTemplate(NEW_SECURITY_TEMPLATE_NAME);
                parentAwareClient.admin().indices().prepareCreate(NEW_INTERNAL_SECURITY_INDEX)
                .addMapping("doc", objMappingAndSettings.v1(), XContentType.JSON)
                .setSettings(objMappingAndSettings.v2())
                .setWaitForActiveShards(ActiveShardCount.ALL)
                .execute(ActionListener.wrap(createObjIndexResponse -> {
                    listener.onResponse(new AcknowledgedResponse(true));
                }, listener::onFailure));
            }, listener::onFailure));
    }

    private Tuple<String, Settings> loadMappingAndSettingsSourceFromTemplate(String templateName) {
        final byte[] template = TemplateUtils
                .loadTemplate("/" + templateName + ".json", Version.CURRENT.toString(), TEMPLATE_VERSION_PATTERN)
                .getBytes(StandardCharsets.UTF_8);
        final PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(template, XContentType.JSON);
        return new Tuple<>(request.mappings().get("doc"), request.settings());
    }

    private void reindex(ParentTaskAssigningClient parentAwareClient, String sourceIndex, String destIndex, QueryBuilder queryBuilder,
            ActionListener<BulkByScrollResponse> listener) {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndex);
        reindexRequest.setSourceDocTypes("doc");
        reindexRequest.setDestIndex(destIndex);
        reindexRequest.setSourceQuery(queryBuilder);
        reindexRequest.setRefresh(true);
        parentAwareClient.execute(ReindexAction.INSTANCE, reindexRequest, listener);
    }

    private void removeReadOnlyBlock(ParentTaskAssigningClient parentAwareClient, String index,
            ActionListener<AcknowledgedResponse> listener) {
        Settings settings = Settings.builder().put(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey(), false).build();
        parentAwareClient.admin().indices().prepareUpdateSettings(index).setSettings(settings).execute(listener);
    }

    /**
     * Makes the index readonly if it's not set as a readonly yet
     */
    private void setReadOnlyBlock(String index, ActionListener<TransportResponse.Empty> listener) {
        clusterService.submitStateUpdateTask("lock-index-for-upgrade", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                final IndexMetaData indexMetaData = currentState.metaData().index(index);
                if (indexMetaData == null) {
                    throw new IndexNotFoundException(index);
                }

                if (indexMetaData.getState() != IndexMetaData.State.OPEN) {
                    throw new IllegalStateException("unable to upgrade a closed index[" + index + "]");
                }
                if (currentState.blocks().hasIndexBlock(index, IndexMetaData.INDEX_READ_ONLY_BLOCK)) {
                    throw new IllegalStateException("unable to upgrade a read-only index[" + index + "]");
                }

                final Settings indexSettingsBuilder =
                        Settings.builder()
                                .put(indexMetaData.getSettings())
                                .put(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey(), true)
                                .build();
                final IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData).settings(indexSettingsBuilder);
                assert same(indexMetaData.getSettings(), indexSettingsBuilder) == false;
                builder.settingsVersion(1 + builder.settingsVersion());

                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData()).put(builder);

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks())
                        .addIndexBlock(index, IndexMetaData.INDEX_READ_ONLY_BLOCK);

                return ClusterState.builder(currentState).metaData(metaDataBuilder).blocks(blocks).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            }
        });
    }
}
