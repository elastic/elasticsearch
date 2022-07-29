/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.common.Strings;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.metadata.ContactPerson;
import org.opensaml.saml.saml2.metadata.EmailAddress;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.GivenName;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.KeyDescriptor;
import org.opensaml.saml.saml2.metadata.NameIDFormat;
import org.opensaml.saml.saml2.metadata.Organization;
import org.opensaml.saml.saml2.metadata.OrganizationDisplayName;
import org.opensaml.saml.saml2.metadata.OrganizationName;
import org.opensaml.saml.saml2.metadata.OrganizationURL;
import org.opensaml.saml.saml2.metadata.SingleLogoutService;
import org.opensaml.saml.saml2.metadata.SingleSignOnService;
import org.opensaml.saml.saml2.metadata.SurName;
import org.opensaml.saml.saml2.metadata.impl.ContactPersonBuilder;
import org.opensaml.saml.saml2.metadata.impl.EmailAddressBuilder;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorBuilder;
import org.opensaml.saml.saml2.metadata.impl.GivenNameBuilder;
import org.opensaml.saml.saml2.metadata.impl.IDPSSODescriptorBuilder;
import org.opensaml.saml.saml2.metadata.impl.KeyDescriptorBuilder;
import org.opensaml.saml.saml2.metadata.impl.NameIDFormatBuilder;
import org.opensaml.saml.saml2.metadata.impl.OrganizationBuilder;
import org.opensaml.saml.saml2.metadata.impl.OrganizationDisplayNameBuilder;
import org.opensaml.saml.saml2.metadata.impl.OrganizationNameBuilder;
import org.opensaml.saml.saml2.metadata.impl.OrganizationURLBuilder;
import org.opensaml.saml.saml2.metadata.impl.SingleLogoutServiceBuilder;
import org.opensaml.saml.saml2.metadata.impl.SingleSignOnServiceBuilder;
import org.opensaml.saml.saml2.metadata.impl.SurNameBuilder;
import org.opensaml.security.credential.UsageType;
import org.opensaml.xmlsec.keyinfo.KeyInfoSupport;
import org.opensaml.xmlsec.signature.KeyInfo;
import org.opensaml.xmlsec.signature.impl.KeyInfoBuilder;

import java.net.URL;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class SamlIdPMetadataBuilder {

    private Locale locale;
    private final String entityId;
    private Set<String> nameIdFormats;
    private boolean wantAuthnRequestsSigned;
    private Map<String, URL> singleSignOnServiceUrls = new HashMap<>();
    private Map<String, URL> singleLogoutServiceUrls = new HashMap<>();
    private List<X509Certificate> signingCertificates;
    private SamlIdentityProvider.OrganizationInfo organization;
    private final List<SamlIdentityProvider.ContactInfo> contacts;

    public SamlIdPMetadataBuilder(String entityId) {
        this.entityId = entityId;
        this.locale = Locale.getDefault();
        this.contacts = new ArrayList<>();
        this.nameIdFormats = new HashSet<>();
        this.signingCertificates = new ArrayList<>();
        this.wantAuthnRequestsSigned = false;
    }

    public SamlIdPMetadataBuilder withLocale(Locale locale) {
        this.locale = locale;
        return this;
    }

    public SamlIdPMetadataBuilder withNameIdFormat(String nameIdFormat) {
        if (Strings.isNullOrEmpty(nameIdFormat) == false) {
            this.nameIdFormats.add(nameIdFormat);
        }
        return this;
    }

    public SamlIdPMetadataBuilder wantAuthnRequestsSigned(boolean wants) {
        this.wantAuthnRequestsSigned = wants;
        return this;
    }

    public SamlIdPMetadataBuilder withSingleSignOnServiceUrl(String binding, URL url) {
        if (null != url) {
            this.singleSignOnServiceUrls.put(binding, url);
        }
        return this;
    }

    public SamlIdPMetadataBuilder withSingleLogoutServiceUrl(String binding, URL url) {
        if (null != url) {
            this.singleLogoutServiceUrls.put(binding, url);
        }
        return this;
    }

    public SamlIdPMetadataBuilder withSigningCertificates(List<X509Certificate> signingCertificates) {
        if (null != signingCertificates) {
            this.signingCertificates.addAll(signingCertificates);
        }
        return this;
    }

    public SamlIdPMetadataBuilder withSigningCertificate(X509Certificate signingCertificate) {
        if (null != signingCertificate) {
            return withSigningCertificates(Collections.singletonList(signingCertificate));
        }
        return this;
    }

    public SamlIdPMetadataBuilder organization(SamlIdentityProvider.OrganizationInfo organization) {
        if (null != organization) {
            this.organization = organization;
        }
        return this;
    }

    public SamlIdPMetadataBuilder organization(String orgName, String displayName, String url) {
        return organization(new SamlIdentityProvider.OrganizationInfo(orgName, displayName, url));
    }

    public SamlIdPMetadataBuilder withContact(SamlIdentityProvider.ContactInfo contact) {
        if (null != contact) {
            this.contacts.add(contact);
        }
        return this;
    }

    public SamlIdPMetadataBuilder withContact(String type, String givenName, String surName, String email) {
        return withContact(new SamlIdentityProvider.ContactInfo(SamlIdentityProvider.ContactInfo.getType(type), givenName, surName, email));
    }

    public EntityDescriptor build() throws CertificateEncodingException {
        final IDPSSODescriptor idpSsoDescriptor = new IDPSSODescriptorBuilder().buildObject();
        idpSsoDescriptor.removeAllSupportedProtocols();
        idpSsoDescriptor.addSupportedProtocol(SAMLConstants.SAML20P_NS);
        idpSsoDescriptor.setWantAuthnRequestsSigned(this.wantAuthnRequestsSigned);
        if (nameIdFormats.isEmpty() == false) {
            idpSsoDescriptor.getNameIDFormats().addAll(buildNameIDFormats());
        }
        if (singleSignOnServiceUrls.isEmpty() == false) {
            idpSsoDescriptor.getSingleSignOnServices().addAll(buildSingleSignOnServices());
        }
        if (singleLogoutServiceUrls.isEmpty() == false) {
            idpSsoDescriptor.getSingleLogoutServices().addAll(buildSingleLogoutServices());
        }
        idpSsoDescriptor.getKeyDescriptors().addAll(buildKeyDescriptors());

        final EntityDescriptor descriptor = new EntityDescriptorBuilder().buildObject();
        descriptor.setEntityID(this.entityId);
        descriptor.getRoleDescriptors().add(idpSsoDescriptor);
        if (organization != null) {
            descriptor.setOrganization(buildOrganization());
        }
        for (SamlIdentityProvider.ContactInfo contact : contacts) {
            descriptor.getContactPersons().add(buildContact(contact));
        }

        return descriptor;
    }

    private List<SingleSignOnService> buildSingleSignOnServices() {
        List<SingleSignOnService> ssoServices = new ArrayList<>();
        if (singleSignOnServiceUrls.isEmpty()) {
            throw new IllegalStateException("At least one SingleSignOnService URL should be specified");
        }
        for (Map.Entry<String, URL> entry : singleSignOnServiceUrls.entrySet()) {
            final SingleSignOnService sso = new SingleSignOnServiceBuilder().buildObject();
            sso.setBinding(entry.getKey());
            sso.setLocation(entry.getValue().toString());
            ssoServices.add(sso);
        }
        return ssoServices;
    }

    private List<SingleLogoutService> buildSingleLogoutServices() {
        List<SingleLogoutService> sloServices = new ArrayList<>();
        for (Map.Entry<String, URL> entry : singleLogoutServiceUrls.entrySet()) {
            final SingleLogoutService slo = new SingleLogoutServiceBuilder().buildObject();
            slo.setBinding(entry.getKey());
            slo.setLocation(entry.getValue().toString());
            sloServices.add(slo);
        }
        return sloServices;
    }

    private List<NameIDFormat> buildNameIDFormats() {
        List<NameIDFormat> formats = new ArrayList<>();
        if (nameIdFormats.isEmpty()) {
            throw new IllegalStateException("NameID format has not been specified");
        }
        for (String nameIdFormat : nameIdFormats) {
            final NameIDFormat format = new NameIDFormatBuilder().buildObject();
            format.setURI(nameIdFormat);
            formats.add(format);

        }
        return formats;
    }

    private List<? extends KeyDescriptor> buildKeyDescriptors() throws CertificateEncodingException {
        if (signingCertificates.isEmpty()) {
            return Collections.emptyList();
        }
        List<KeyDescriptor> keys = new ArrayList<>();
        for (X509Certificate signingCertificate : signingCertificates) {
            if (signingCertificate != null) {
                final KeyDescriptor descriptor = new KeyDescriptorBuilder().buildObject();
                descriptor.setUse(UsageType.SIGNING);
                final KeyInfo keyInfo = new KeyInfoBuilder().buildObject();
                KeyInfoSupport.addCertificate(keyInfo, signingCertificate);
                descriptor.setKeyInfo(keyInfo);
                keys.add(descriptor);
            }
        }
        return keys;
    }

    private Organization buildOrganization() {
        final String lang = locale.toLanguageTag();
        final OrganizationName name = new OrganizationNameBuilder().buildObject();
        name.setValue(this.organization.organizationName);
        name.setXMLLang(lang);
        final OrganizationDisplayName displayName = new OrganizationDisplayNameBuilder().buildObject();
        displayName.setValue(this.organization.displayName);
        displayName.setXMLLang(lang);
        final OrganizationURL url = new OrganizationURLBuilder().buildObject();
        url.setURI(this.organization.url);
        url.setXMLLang(lang);

        final Organization org = new OrganizationBuilder().buildObject();
        org.getOrganizationNames().add(name);
        org.getDisplayNames().add(displayName);
        org.getURLs().add(url);
        return org;
    }

    private ContactPerson buildContact(SamlIdentityProvider.ContactInfo contact) {
        final GivenName givenName = new GivenNameBuilder().buildObject();
        givenName.setValue(contact.givenName);
        final SurName surName = new SurNameBuilder().buildObject();
        surName.setValue(contact.surName);
        final EmailAddress email = new EmailAddressBuilder().buildObject();
        email.setURI(contact.email);

        final ContactPerson person = new ContactPersonBuilder().buildObject();
        person.setType(contact.type);
        person.setGivenName(givenName);
        person.setSurName(surName);
        person.getEmailAddresses().add(email);
        return person;
    }
}
