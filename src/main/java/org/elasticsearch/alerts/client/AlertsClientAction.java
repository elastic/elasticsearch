/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.client;

import org.elasticsearch.action.*;
import org.elasticsearch.alerts.Alert;

/**
 * Created by brian on 10/29/14.
 */

public abstract class AlertsClientAction<Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, AlertsClientInterface>>
        extends Action<Request, Response, RequestBuilder, AlertsClientInterface> {

    protected AlertsClientAction(String name) {
        super(name);
    }

}