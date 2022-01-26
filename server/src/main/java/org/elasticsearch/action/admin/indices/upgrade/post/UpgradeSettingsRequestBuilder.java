/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.core.Tuple;

import java.util.Map;

/**
 * Builder for an update index settings request
 */
public class UpgradeSettingsRequestBuilder extends AcknowledgedRequestBuilder<
    UpgradeSettingsRequest,
    AcknowledgedResponse,
    UpgradeSettingsRequestBuilder> {

    public UpgradeSettingsRequestBuilder(ElasticsearchClient client, UpgradeSettingsAction action) {
        super(client, action, new UpgradeSettingsRequest());
    }

    /**
     * Sets the index versions to be updated
     */
    public UpgradeSettingsRequestBuilder setVersions(Map<String, Tuple<Version, String>> versions) {
        request.versions(versions);
        return this;
    }
}
