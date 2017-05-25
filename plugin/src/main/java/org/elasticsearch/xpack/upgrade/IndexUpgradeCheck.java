/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.script.Script;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Generic upgrade check applicable to all indices to be upgraded from the current version
 * to the next major version
 * <p>
 * The upgrade is performed in the following way:
 * <p>
 * - preUpgrade method is called
 * - reindex is performed
 * - postUpgrade is called if reindex was successful
 */
public class IndexUpgradeCheck extends AbstractComponent {
    public static final int UPRADE_VERSION = 6;

    private final String name;
    private final Predicate<Tuple<IndexMetaData, Map<String, String>>> isSupported;
    private final InternalIndexReindexer reindexer;
    private final UpgradeActionRequired matchAction;
    private final UpgradeActionRequired noMatchAction;

    /**
     * Creates a new upgrade check that doesn't support upgrade
     *
     * @param name            - the name of the check
     * @param settings        - system settings
     * @param isSupported     - return true if they can work with the index with specified name
     * @param matchAction     - action if isSupported return true
     * @param noMatchAction     - action if isSupported return false
     */
    public IndexUpgradeCheck(String name, Settings settings,
                             Predicate<Tuple<IndexMetaData, Map<String, String>>> isSupported,
                             UpgradeActionRequired matchAction, UpgradeActionRequired noMatchAction) {
        super(settings);
        this.name = name;
        this.isSupported = isSupported;
        this.reindexer = null;
        this.matchAction = matchAction;
        this.noMatchAction = noMatchAction;
    }

    /**
     * Creates a new upgrade check
     *
     * @param name            - the name of the check
     * @param settings        - system settings
     * @param client          - client
     * @param clusterService  - cluster service
     * @param isSupported     - return true if they can work with the index with specified name
     * @param types           - a list of types that the reindexing should be limited to
     * @param updateScript    - the upgrade script that should be used during reindexing
     */
    public IndexUpgradeCheck(String name, Settings settings, Client client, ClusterService clusterService,
                             Predicate<Tuple<IndexMetaData, Map<String, String>>> isSupported,
                             String[] types, Script updateScript) {
        this(name, settings, client, clusterService, isSupported, types, updateScript, UpgradeActionRequired.UPGRADE,
                UpgradeActionRequired.NOT_APPLICABLE);
    }

    /**
     * Creates a new upgrade check
     *
     * @param name            - the name of the check
     * @param settings        - system settings
     * @param client          - client
     * @param clusterService  - cluster service
     * @param isSupported     - return true if they can work with the index with specified name
     * @param types           - a list of types that the reindexing should be limited to
     * @param updateScript    - the upgrade script that should be used during reindexing
     */
    public IndexUpgradeCheck(String name, Settings settings, Client client, ClusterService clusterService,
                             Predicate<Tuple<IndexMetaData, Map<String, String>>> isSupported,
                             String[] types, Script updateScript, UpgradeActionRequired matchAction, UpgradeActionRequired noMatchAction) {
        super(settings);
        this.name = name;
        this.isSupported = isSupported;
        this.reindexer = new InternalIndexReindexer(client, clusterService, UPRADE_VERSION, updateScript, types);
        this.matchAction = matchAction;
        this.noMatchAction = noMatchAction;
    }
    /**
     * Returns the name of the check
     */
    public String getName() {
        return name;
    }

    /**
     * This method is called by Upgrade API to verify if upgrade or reindex for this index is required
     *
     * @param indexMetaData index metadata
     * @param params        additional user-specified parameters see {@link IndexUpgradeCheckFactory#supportedParams}
     * @param state         current cluster state
     * @return required action or UpgradeActionRequired.NOT_APPLICABLE if this check cannot be performed on the index
     */
    public UpgradeActionRequired actionRequired(IndexMetaData indexMetaData, Map<String, String> params, ClusterState state) {
        if (isSupported.test(new Tuple<>(indexMetaData, params))) {
            return matchAction;
        }
        return noMatchAction;
    }

    /**
     * Perform the index upgrade
     *
     * @param indexMetaData index metadata
     * @param params        additional user-specified parameters see {@link IndexUpgradeCheckFactory#supportedParams}
     * @param state         current cluster state
     * @param listener      the listener that should be called upon completion of the upgrade
     */
    public void upgrade(IndexMetaData indexMetaData, Map<String, String> params, ClusterState state,
                        ActionListener<BulkByScrollResponse> listener) {
        if (reindexer == null) {
            throw new UnsupportedOperationException(getName() + " check doesn't support index upgrade");
        } else {
            reindexer.upgrade(indexMetaData.getIndex().getName(), state, listener);
        }
    }

}
