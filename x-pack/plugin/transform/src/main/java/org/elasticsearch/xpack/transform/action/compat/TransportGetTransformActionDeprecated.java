/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action.compat;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.compat.GetTransformActionDeprecated;
import org.elasticsearch.xpack.transform.action.TransportGetTransformAction;

public class TransportGetTransformActionDeprecated extends TransportGetTransformAction {

    @Inject
    public TransportGetTransformActionDeprecated(TransportService transportService, ActionFilters actionFilters, Client client,
            NamedXContentRegistry xContentRegistry) {
        super(GetTransformActionDeprecated.NAME, transportService, actionFilters, client, xContentRegistry);
    }

}
