/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.joda.time.ReadableDuration;

import java.net.URI;

/**
 * SAML 2.0 configuration information about a specific service provider
 */
public interface SamlServiceProvider {
    String getEntityId();

    URI getAssertionConsumerService();

    ReadableDuration getAuthnExpiry();

    class AttributeNames {
        public final String groups = "https://saml.elasticsearch.org/attributes/groups";
    }

    AttributeNames getAttributeNames();

    ServiceProviderPrivileges getPrivileges();
}
