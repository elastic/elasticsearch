/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.DocumentVersion;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Before;
import org.opensaml.saml.saml2.core.NameID;

import java.net.URL;
import java.util.Set;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SamlServiceProviderResolverTests extends ESTestCase {

    private SamlServiceProviderIndex index;
    private ServiceProviderDefaults serviceProviderDefaults;
    private SamlIdentityProvider identityProvider;
    private SamlServiceProviderResolver resolver;

    @Before
    public void setupMocks() {
        index = mock(SamlServiceProviderIndex.class);
        identityProvider = mock(SamlIdentityProvider.class);
        serviceProviderDefaults = configureIdentityProviderDefaults();
        resolver = new SamlServiceProviderResolver(Settings.EMPTY, index, new SamlServiceProviderFactory(serviceProviderDefaults));
    }

    public void testResolveWithoutCache() throws Exception {

        final String entityId = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/";
        final URL acs = new URL(entityId + "saml/acs");

        final String principalAttribute = randomAlphaOfLengthBetween(6, 36);
        final String rolesAttribute = randomAlphaOfLengthBetween(6, 36);
        final String resource = "ece:" + randomAlphaOfLengthBetween(6, 12);
        final Set<String> rolePrivileges = Set.of("role:(.*)");

        final DocumentVersion docVersion = new DocumentVersion(
            randomAlphaOfLength(12), randomNonNegativeLong(), randomNonNegativeLong());
        final SamlServiceProviderDocument document = new SamlServiceProviderDocument();
        document.setEntityId(entityId);
        document.setAuthenticationExpiry(null);
        document.setAcs(acs.toString());
        document.privileges.setResource(resource);
        document.privileges.setRolePatterns(rolePrivileges);
        document.attributeNames.setPrincipal(principalAttribute);
        document.attributeNames.setRoles(rolesAttribute);

        mockDocument(entityId, docVersion, document);

        final SamlServiceProvider serviceProvider = resolveServiceProvider(entityId);
        assertThat(serviceProvider.getEntityId(), equalTo(entityId));
        assertThat(serviceProvider.getAssertionConsumerService(), equalTo(acs));
        assertThat(serviceProvider.getAllowedNameIdFormat(), equalTo(serviceProviderDefaults.nameIdFormat));
        assertThat(serviceProvider.getAuthnExpiry(), equalTo(serviceProviderDefaults.authenticationExpiry));
        assertThat(serviceProvider.getSpSigningCredentials(), emptyIterable());
        assertThat(serviceProvider.shouldSignAuthnRequests(), equalTo(false));
        assertThat(serviceProvider.shouldSignLogoutRequests(), equalTo(false));

        assertThat(serviceProvider.getAttributeNames(), notNullValue());
        assertThat(serviceProvider.getAttributeNames().principal, equalTo(principalAttribute));
        assertThat(serviceProvider.getAttributeNames().name, nullValue());
        assertThat(serviceProvider.getAttributeNames().email, nullValue());
        assertThat(serviceProvider.getAttributeNames().roles, equalTo(rolesAttribute));

        assertThat(serviceProvider.getPrivileges(), notNullValue());
        assertThat(serviceProvider.getPrivileges().getApplicationName(), equalTo(serviceProviderDefaults.applicationName));
        assertThat(serviceProvider.getPrivileges().getResource(), equalTo(resource));
        assertThat(serviceProvider.getPrivileges().getRoleMapping(), notNullValue());
        assertThat(serviceProvider.getPrivileges().getRoleMapping().apply("role:foo"), equalTo(Set.of("foo")));
        assertThat(serviceProvider.getPrivileges().getRoleMapping().apply("foo:bar"), equalTo(Set.of()));
    }

    public void testResolveReturnsCachedObject() throws Exception {
        final SamlServiceProviderDocument document1 = SamlServiceProviderTestUtils.randomDocument(1);
        final SamlServiceProviderDocument document2 = SamlServiceProviderTestUtils.randomDocument(2);
        document2.entityId = document1.entityId;

        final DocumentVersion docVersion = new DocumentVersion(randomAlphaOfLength(12), 1, 1);

        mockDocument(document1.entityId, docVersion, document1);
        final SamlServiceProvider serviceProvider1 = resolveServiceProvider(document1.entityId);

        mockDocument(document1.entityId, docVersion, document2);
        final SamlServiceProvider serviceProvider2 = resolveServiceProvider(document1.entityId);

        assertThat(serviceProvider2, sameInstance(serviceProvider1));
    }

    public void testResolveIgnoresCacheWhenDocumentVersionChanges() throws Exception {
        final SamlServiceProviderDocument document1 = SamlServiceProviderTestUtils.randomDocument(1);
        final SamlServiceProviderDocument document2 = SamlServiceProviderTestUtils.randomDocument(2);
        document2.entityId = document1.entityId;

        final DocumentVersion docVersion1 = new DocumentVersion(randomAlphaOfLength(12), 1, 1);
        final DocumentVersion docVersion2 = new DocumentVersion(randomAlphaOfLength(12), randomIntBetween(2, 10), randomIntBetween(1, 10));

        mockDocument(document1.entityId, docVersion1, document1);
        final SamlServiceProvider serviceProvider1a = resolveServiceProvider(document1.entityId);
        final SamlServiceProvider serviceProvider1b = resolveServiceProvider(document1.entityId);
        assertThat(serviceProvider1b, sameInstance(serviceProvider1a));

        mockDocument(document1.entityId, docVersion2, document2);
        final SamlServiceProvider serviceProvider2 = resolveServiceProvider(document1.entityId);

        assertThat(serviceProvider2, not(sameInstance(serviceProvider1a)));
        assertThat(serviceProvider2.getEntityId(), equalTo(document2.entityId));
        assertThat(serviceProvider2.getAssertionConsumerService().toString(), equalTo(document2.acs));
        assertThat(serviceProvider2.getAttributeNames().principal, equalTo(document2.attributeNames.principal));
        assertThat(serviceProvider2.getAttributeNames().name, equalTo(document2.attributeNames.name));
        assertThat(serviceProvider2.getAttributeNames().email, equalTo(document2.attributeNames.email));
        assertThat(serviceProvider2.getAttributeNames().roles, equalTo(document2.attributeNames.roles));
        assertThat(serviceProvider2.getPrivileges().getResource(), equalTo(document2.privileges.resource));
    }

    private SamlServiceProvider resolveServiceProvider(String entityId) {
        final PlainActionFuture<SamlServiceProvider> future = new PlainActionFuture<>();
        resolver.resolve(entityId, future);

        final SamlServiceProvider serviceProvider = future.actionGet();
        assertThat(serviceProvider, notNullValue());
        return serviceProvider;
    }

    private ServiceProviderDefaults configureIdentityProviderDefaults() {
        final String defaultNameId = NameID.TRANSIENT;
        final String defaultApplication = randomAlphaOfLengthBetween(4, 12);
        final Duration defaultExpiry = Duration.standardMinutes(12);
        final ServiceProviderDefaults defaults = new ServiceProviderDefaults(
            defaultApplication, defaultNameId, defaultExpiry);
        when(identityProvider.getServiceProviderDefaults()).thenReturn(defaults);
        return defaults;
    }

    @SuppressWarnings("unchecked")
    private void mockDocument(String entityId, DocumentVersion docVersion, SamlServiceProviderDocument document) {
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, Matchers.arrayWithSize(2));

            assertThat(args[0], equalTo(entityId));

            ActionListener<Set<SamlServiceProviderIndex.DocumentSupplier>> listener
                = (ActionListener<Set<SamlServiceProviderIndex.DocumentSupplier>>) args[args.length - 1];
            listener.onResponse(Set.of(new SamlServiceProviderIndex.DocumentSupplier(docVersion, () -> document)));
            return null;
        }).when(index).findByEntityId(anyString(), any(ActionListener.class));
    }
}
