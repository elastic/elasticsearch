/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
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
    private static final String NEW_SECURITY_TEMPLATE_NAME = "security-index-template-" + (INTERNAL_SECURITY_INDEX_FORMAT + 1);

    private static final String SECURITY_TOKENS_ALIAS_NAME = ".security-tokens";
    private static final int INTERNAL_SECURITY_TOKENS_INDEX_FORMAT = 7;
    private static final String INTERNAL_SECURITY_TOKENS_INDEX = ".security-tokens-" + INTERNAL_SECURITY_TOKENS_INDEX_FORMAT;
    private static final String SECURITY_TOKENS_TEMPLATE_NAME = "security-tokens-index-template-" + INTERNAL_SECURITY_TOKENS_INDEX_FORMAT;

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
     * Perform the security index upgrade. The `.security-6` index is reindexed into `.security-tokens-7` and `.security-7`. The two reindex
     * operations are issued sequentially, firstly the reindex from `.security-6` into `.security-tokens-7` of all token documents and then,
     * secondly, from `.security-6` into `.security-7` of all non-token documents (roles, users, etc). During this, the `.security-6` is
     * marked read-only, and thereafter it is removed. For this reason, tokens cannot be created/invalidated/refreshed (but can be used) in
     * the span of time of the first reindex operation. Passwords, roles and role mappings cannot be changed during the whole upgrade
     * operation. Normally, token docs are more write intensive compared with the other docs in the `.security-6` index, and this strategy
     * aims to minimize the window of time during which certain token operations are unavailable. In case of any unexpected failure in this
     * flow no other rectifying action is taken, besides toggling back the read-write flag on `.security-6` - manual intervention is
     * required in case of failure, the upgrade cannot be simply rerun.
     *
     * @param task
     *            the task that executes the upgrade operation
     * @param indexMetaData
     *            index metadata of the `.security-6` index
     * @param state
     *            current cluster state
     * @param listener
     *            The listener that should be called upon completion of the upgrade. The listener's response will be populated with the
     *            results of the two reindex operations.
     */
    @Override
    public void upgrade(TaskId task, IndexMetaData indexMetaData, ClusterState state, ActionListener<BulkByScrollResponse> listener) {
        final ParentTaskAssigningClient parentAwareClient = new ParentTaskAssigningClient(client, task);
        final BoolQueryBuilder tokensQuery = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", "token"));
        // invalidated-token docs are abandoned as they are not used anymore
        final BoolQueryBuilder nonTokensQuery = QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("doc_type", "token")).mustNot(QueryBuilders.termQuery("doc_type", "invalidated-token")));
        // in case of failure, make sure we don't forget to restore the .security-6 to read-write
        final Consumer<Exception> removeReadOnlyBlock = e -> removeReadOnlyBlock(parentAwareClient, INTERNAL_SECURITY_INDEX,
                ActionListener.wrap(unsetReadOnlyResponse -> {
                    listener.onFailure(e);
                }, e1 -> {
                    e.addSuppressed(e1);
                    listener.onFailure(e);
                }));

        // check all nodes can handle the new index format
        if (state.nodes().getMinNodeVersion().before(Upgrade.UPGRADE_INTRODUCED)) {
            listener.onFailure(
                    new IllegalStateException("All nodes should have at least version [" + Upgrade.UPGRADE_INTRODUCED + "] to upgrade"));
        }

        // create the two new .security-* indices, but without their corresponding aliases
        final StepListener<AcknowledgedResponse> createNewSecurityIndicesStep = createNewSecurityIndices(parentAwareClient);

        final StepListener<TransportResponse.Empty> setReadOnlyStep = new StepListener<>();
        createNewSecurityIndicesStep.whenComplete(createIndicesResponse -> {
            // set a read-only block on the original .security-6 index
            setReadOnlyBlock(INTERNAL_SECURITY_INDEX, setReadOnlyStep);
        }, listener::onFailure);

        final StepListener<BulkByScrollResponse> reindexTokensStep = new StepListener<>();
        setReadOnlyStep.whenComplete(setReadOnlyResponse -> {
            // firstly, reindex all the tokens to the new tokens index
            reindex(parentAwareClient, INTERNAL_SECURITY_INDEX, INTERNAL_SECURITY_TOKENS_INDEX, tokensQuery, reindexTokensStep);
        }, listener::onFailure);

        final StepListener<AcknowledgedResponse> createTokensAliasStep = new StepListener<>();
        reindexTokensStep.whenComplete(reindexTokensResponse -> {
            // then, create the alias .security-tokens pointing to the new tokens index; now the new tokens index/alias is writable anew
            parentAwareClient.admin().indices().prepareAliases().addAlias(INTERNAL_SECURITY_TOKENS_INDEX, SECURITY_TOKENS_ALIAS_NAME)
                    .execute(createTokensAliasStep);
        }, removeReadOnlyBlock);

        final StepListener<BulkByScrollResponse> reindexNonTokensStep = new StepListener<>();
        createTokensAliasStep.whenComplete(tokensAliasResponse -> {
            // now reindex all the other non-token security objects to the new .security-7 index
            reindex(parentAwareClient, INTERNAL_SECURITY_INDEX, NEW_INTERNAL_SECURITY_INDEX, nonTokensQuery, reindexNonTokensStep);
        }, removeReadOnlyBlock);

        final StepListener<AcknowledgedResponse> createNonTokensAliasStep = new StepListener<>();
        reindexNonTokensStep.whenComplete(reindexNonTokensResponse -> {
            // move the .security alias to the new .security-7 index, and remove the old .security-6 index
            parentAwareClient.admin().indices().prepareAliases().addAlias(NEW_INTERNAL_SECURITY_INDEX, SECURITY_ALIAS_NAME)
                    .removeIndex(INTERNAL_SECURITY_INDEX).execute(createNonTokensAliasStep);
        }, removeReadOnlyBlock);

        createNonTokensAliasStep.whenComplete(aliasResponse -> {
            // merge both reindex responses to return to the action listener
            listener.onResponse(new BulkByScrollResponse(Arrays.asList(reindexTokensStep.result(), reindexNonTokensStep.result()), null));
        }, removeReadOnlyBlock);
    }

    private StepListener<AcknowledgedResponse> createNewSecurityIndices(ParentTaskAssigningClient parentAwareClient) {
        final StepListener<AcknowledgedResponse> createNewSecurityIndicesStep = new StepListener<>();
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
                    createNewSecurityIndicesStep.onResponse(new AcknowledgedResponse(true));
                }, createNewSecurityIndicesStep::onFailure));
            }, createNewSecurityIndicesStep::onFailure));
        return createNewSecurityIndicesStep;
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
        reindexRequest.setDestDocType("doc");
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

                return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
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
