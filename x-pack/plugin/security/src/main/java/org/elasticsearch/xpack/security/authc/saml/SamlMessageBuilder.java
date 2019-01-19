/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.metadata.Endpoint;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;

/**
 * Abstract base class for object that build some sort of {@link org.opensaml.saml.common.SAMLObject}
 */
public abstract class SamlMessageBuilder {

    protected final Logger logger;
    protected final Clock clock;
    protected final SpConfiguration serviceProvider;
    protected final EntityDescriptor identityProvider;

    public SamlMessageBuilder(EntityDescriptor identityProvider, SpConfiguration serviceProvider, Clock clock) {
        this.logger = LogManager.getLogger(getClass());
        this.identityProvider = identityProvider;
        this.serviceProvider = serviceProvider;
        this.clock = clock;
    }

    protected String getIdentityProviderEndpoint(String binding,
                                                 Function<IDPSSODescriptor, ? extends Collection<? extends Endpoint>> selector) {
        final List<String> locations = identityProvider.getRoleDescriptors(IDPSSODescriptor.DEFAULT_ELEMENT_NAME).stream()
                .map(rd -> (IDPSSODescriptor) rd)
                .flatMap(idp -> selector.apply(idp).stream())
                .filter(endp -> binding.equals(endp.getBinding()))
                .map(sso -> sso.getLocation())
                .collect(Collectors.toList());
        if (locations.isEmpty()) {
            return null;
        }
        if (locations.size() > 1) {
            throw new ElasticsearchException("Found multiple locations for binding [{}] in descriptor [{}] - [{}]",
                    binding, identityProvider.getID(), locations);
        }
        return locations.get(0);
    }

    protected DateTime now() {
        return new DateTime(clock.millis(), DateTimeZone.UTC);
    }

    protected Issuer buildIssuer() {
        Issuer issuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(this.serviceProvider.getEntityId());
        return issuer;
    }

    protected String buildId() {
        // 20 bytes (160 bits) of randomness as recommended by the SAML spec
        return SamlUtils.generateSecureNCName(20);
    }
}
