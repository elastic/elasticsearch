/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteTransportException;

public class ElasticsearchLicenseException extends RemoteTransportException {


    public ElasticsearchLicenseException(String msg) {
        super(msg, null);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
