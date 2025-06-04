/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.exception.ElasticsearchException;

/**
 * Indicates a configuration or execution problem specific to a SAML ServiceProvider
 */
public class ServiceProviderException extends ElasticsearchException {
    public static final String ENTITY_ID = "es.idp.sp.entity_id";

    public ServiceProviderException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public void setEntityId(String entityId) {
        super.addMetadata(ENTITY_ID, entityId);
    }
}
