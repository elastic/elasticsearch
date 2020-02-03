/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.Strings;
import org.joda.time.Duration;
import org.joda.time.ReadableDuration;

import java.net.URI;
import java.net.URISyntaxException;

public class CloudKibanaServiceProvider implements SamlServiceProvider {

    private final String entityid;
    private final URI assertionConsumerService;
    private final ReadableDuration authnExpiry;

    public CloudKibanaServiceProvider(String entityId, String assertionConsumerService) {
        if (Strings.isNullOrEmpty(entityId)) {
            throw new IllegalArgumentException("Service Provider Entity ID cannot be null or empty");
        }
        this.entityid = entityId;
        try {
            this.assertionConsumerService = new URI(assertionConsumerService);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI for Assertion Consumer Service", e);
        }
        this.authnExpiry = Duration.standardMinutes(5);
    }

    @Override
    public String getEntityId() {
        return entityid;
    }

    @Override
    public URI getAssertionConsumerService() {
        return assertionConsumerService;
    }

    @Override
    public ReadableDuration getAuthnExpiry() {
        return authnExpiry;
    }

    @Override
    public AttributeNames getAttributeNames() {
        return new SamlServiceProvider.AttributeNames();
    }
}
