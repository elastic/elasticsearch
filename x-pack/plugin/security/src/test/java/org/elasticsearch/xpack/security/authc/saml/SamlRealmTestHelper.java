/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.SingleLogoutService;
import org.opensaml.security.x509.X509Credential;

import static org.mockito.Mockito.mock;

public class SamlRealmTestHelper {

    public static final String SP_ENTITY_ID = "https://sp.example.net/";
    public static final String SP_ACS_URL = SP_ENTITY_ID + "saml/acs";
    public static final String SP_LOGOUT_URL = SP_ENTITY_ID + "saml/logout";

    public static final String IDP_ENTITY_ID = "https://idp.example.org/";
    public static final String IDP_LOGOUT_URL = IDP_ENTITY_ID + "saml/logout";

    public static SamlRealm buildRealm(RealmConfig realmConfig, @Nullable X509Credential credential) throws Exception {
        EntityDescriptor idpDescriptor = SamlUtils.buildObject(EntityDescriptor.class, EntityDescriptor.DEFAULT_ELEMENT_NAME);
        final IDPSSODescriptor role = SamlUtils.buildObject(IDPSSODescriptor.class, IDPSSODescriptor.DEFAULT_ELEMENT_NAME);
        final SingleLogoutService slo = SamlUtils.buildObject(SingleLogoutService.class, SingleLogoutService.DEFAULT_ELEMENT_NAME);
        idpDescriptor.getRoleDescriptors().add(role);
        role.getSingleLogoutServices().add(slo);
        slo.setBinding(SAMLConstants.SAML2_REDIRECT_BINDING_URI);
        slo.setLocation(IDP_LOGOUT_URL);

        final SpConfiguration spConfiguration = new SpConfiguration(SP_ENTITY_ID, SP_ACS_URL, SP_LOGOUT_URL,
            new SigningConfiguration(Collections.singleton("*"), credential), Arrays.asList(credential), Collections.emptyList());
        return new SamlRealm(realmConfig, mock(UserRoleMapper.class), mock(SamlAuthenticator.class),
            mock(SamlLogoutRequestHandler.class), mock(SamlLogoutResponseHandler.class),
            () -> idpDescriptor, spConfiguration);
    }

    public static void writeIdpMetadata(Path path, String idpEntityId) throws IOException {
        Files.write(path, Arrays.asList(
            "<?xml version=\"1.0\"?>",
            "<md:EntityDescriptor xmlns:md='urn:oasis:names:tc:SAML:2.0:metadata' entityID='" + idpEntityId + "'>",
            "<md:IDPSSODescriptor protocolSupportEnumeration='urn:oasis:names:tc:SAML:2.0:protocol'>",
            "<md:SingleSignOnService Binding='urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect' Location='http://localhost/sso/' />",
            "</md:IDPSSODescriptor>",
            "</md:EntityDescriptor>"
        ));
    }
}
