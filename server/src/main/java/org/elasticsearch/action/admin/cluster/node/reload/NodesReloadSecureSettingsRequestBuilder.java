/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.SecureString;

/**
 * Builder for the reload secure settings nodes request
 */
public class NodesReloadSecureSettingsRequestBuilder extends NodesOperationRequestBuilder<NodesReloadSecureSettingsRequest,
        NodesReloadSecureSettingsResponse, NodesReloadSecureSettingsRequestBuilder> {

    public NodesReloadSecureSettingsRequestBuilder(ElasticsearchClient client, NodesReloadSecureSettingsAction action) {
        super(client, action, new NodesReloadSecureSettingsRequest());
    }

    public NodesReloadSecureSettingsRequestBuilder setSecureStorePassword(SecureString secureStorePassword) {
        request.setSecureStorePassword(secureStorePassword);
        return this;
    }

}
