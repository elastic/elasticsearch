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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.DjbHashFunction;
import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.SimpleHashFunction;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 * This service is responsible for upgrading legacy index metadata to the current version
 *
 * Every time an existing index is introduced into cluster this service should be used
 * to upgrade the existing index metadata to the latest version of the cluster. It typically
 * occurs during cluster upgrade, when dangling indices are imported into the cluster or indices
 * are restored from a repository.
 */
public class MetaDataIndexUpgradeService extends AbstractComponent {

    private static final String DEPRECATED_SETTING_ROUTING_HASH_FUNCTION = "cluster.routing.operation.hash.type";
    private static final String DEPRECATED_SETTING_ROUTING_USE_TYPE = "cluster.routing.operation.use_type";

    private final Class<? extends HashFunction> pre20HashFunction;
    private final Boolean pre20UseType;

    @Inject
    public MetaDataIndexUpgradeService(Settings settings) {
        super(settings);

        final String pre20HashFunctionName = settings.get(DEPRECATED_SETTING_ROUTING_HASH_FUNCTION, null);
        final boolean hasCustomPre20HashFunction = pre20HashFunctionName != null;
        // the hash function package has changed we replace the two hash functions if their fully qualified name is used.
        if (hasCustomPre20HashFunction) {
            switch (pre20HashFunctionName) {
                case "org.elasticsearch.cluster.routing.operation.hash.simple.SimpleHashFunction":
                    pre20HashFunction = SimpleHashFunction.class;
                    break;
                case "org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction":
                    pre20HashFunction = DjbHashFunction.class;
                    break;
                default:
                    pre20HashFunction = settings.getAsClass(DEPRECATED_SETTING_ROUTING_HASH_FUNCTION, DjbHashFunction.class, "org.elasticsearch.cluster.routing.", "HashFunction");
            }
        } else {
            pre20HashFunction = DjbHashFunction.class;
        }
        pre20UseType = settings.getAsBoolean(DEPRECATED_SETTING_ROUTING_USE_TYPE, null);
        if (hasCustomPre20HashFunction|| pre20UseType != null) {
            logger.warn("Settings [{}] and [{}] are deprecated. Index settings from your old indices have been updated to record the fact that they "
                    + "used some custom routing logic, you can now remove these settings from your `elasticsearch.yml` file", DEPRECATED_SETTING_ROUTING_HASH_FUNCTION, DEPRECATED_SETTING_ROUTING_USE_TYPE);
        }
    }

    /**
     * Checks that the index can be upgraded to the current version of the master node.
     *
     * If the index does need upgrade it returns the index metadata unchanged, otherwise it returns a modified index metadata. If index cannot be
     * updated the method throws an exception.
     */
    public IndexMetaData upgradeIndexMetaData(IndexMetaData indexMetaData) throws Exception {
        IndexMetaData newMetaData = indexMetaData;
        newMetaData = checkSupportedVersion(newMetaData);
        newMetaData = upgradeLegacyRoutingSettings(newMetaData);
        return newMetaData;
    }

    /**
     * Elasticsearch 2.0 deprecated no longer supports indices with pre Lucene v4.0 segments. All indices
     * that were created before Elasticsearch v0.90.0 should be upgraded using upgrade plugin before they can
     * be open by this version of elasticsearch.
     */
    private IndexMetaData checkSupportedVersion(IndexMetaData indexMetaData) throws Exception {
        if (indexMetaData.getState() == IndexMetaData.State.OPEN && isSupportedVersion(indexMetaData) == false) {
            throw new IllegalStateException("The index [" + indexMetaData.getIndex() + "] was created before v0.90.0 and wasn't upgraded."
                    + " This index should be open using a version before " + Version.CURRENT.minimumCompatibilityVersion()
                    + " and upgraded using the upgrade API.");
        }
        return indexMetaData;
    }

    /*
     * Returns true if this index can be supported by the current version of elasticsearch
     */
    private static boolean isSupportedVersion(IndexMetaData indexMetaData) {
        return indexMetaData.minimumCompatibleVersion() != null &&
                indexMetaData.minimumCompatibleVersion().luceneVersion.onOrAfter(Version.V_0_90_0_Beta1.luceneVersion);
    }

    /**
     * Elasticsearch 2.0 deprecated custom routing hash functions. So what we do here is that for old indices, we
     * move this old and deprecated node setting to an index setting so that we can keep things backward compatible.
     */
    private IndexMetaData upgradeLegacyRoutingSettings(IndexMetaData indexMetaData) throws Exception {
        if (indexMetaData.settings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION) == null
                && indexMetaData.getCreationVersion().before(Version.V_2_0_0)) {
            // these settings need an upgrade
            Settings indexSettings = Settings.builder().put(indexMetaData.settings())
                    .put(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION, pre20HashFunction)
                    .put(IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE, pre20UseType == null ? false : pre20UseType)
                    .build();
            return IndexMetaData.builder(indexMetaData)
                    .version(indexMetaData.version())
                    .settings(indexSettings)
                    .build();
        } else if (indexMetaData.getCreationVersion().onOrAfter(Version.V_2_0_0)) {
            if (indexMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION) != null
                    || indexMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE) != null) {
                throw new IllegalStateException("Indices created on or after 2.0 should NOT contain [" + IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION
                        + "] + or [" + IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE + "] in their index settings");
            }
        }
        return indexMetaData;
    }

}
