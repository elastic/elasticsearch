/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.metadata.ContactPerson;
import org.opensaml.saml.saml2.metadata.ContactPersonTypeEnumeration;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SamlIdPMetadataBuilder {

    private Locale locale;
    private final String entityId;
    private Set<String> nameIdFormats;
    private boolean wantAuthnRequestsSigned;
    private Map<String, URL> singleSignOnServiceUrls = new HashMap<>();
    private Map<String, URL> singleLogoutServiceUrls = new HashMap<>();
    private List<X509Certificate> signingCertificates;
    private OrganizationInfo organization;
    private final List<ContactInfo> contacts;


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
        if ( null != url) {
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
        return withSigningCertificates(Collections.singletonList(signingCertificate));
    }

    public SamlIdPMetadataBuilder organization(OrganizationInfo organization) {
        if (null != organization) {
            this.organization = organization;
        }
        return this;
    }

    public SamlIdPMetadataBuilder organization(String orgName, String displayName, String url) {
        return organization(new OrganizationInfo(orgName, displayName, url));
    }

    public SamlIdPMetadataBuilder withContact(ContactInfo contact) {
        if (null != contact) {
            this.contacts.add(contact);
        }
        return this;
    }

    public SamlIdPMetadataBuilder withContact(String type, String givenName, String surName, String email) {
        return withContact(new ContactInfo(ContactInfo.getType(type), givenName, surName, email));
    }


    public EntityDescriptor build() throws CertificateEncodingException {
        final IDPSSODescriptor idpssoDescriptor = new IDPSSODescriptorBuilder().buildObject();
        idpssoDescriptor.removeAllSupportedProtocols();
        idpssoDescriptor.addSupportedProtocol(SAMLConstants.SAML20P_NS);
        idpssoDescriptor.setWantAuthnRequestsSigned(this.wantAuthnRequestsSigned);
        if (nameIdFormats.isEmpty() == false) {
            idpssoDescriptor.getNameIDFormats().addAll(buildNameIDFormats());
        }
        if (singleSignOnServiceUrls.isEmpty() == false) {
            idpssoDescriptor.getSingleSignOnServices().addAll(buildSingleSignOnServices());
        }
        if (singleLogoutServiceUrls.isEmpty() == false) {
            idpssoDescriptor.getSingleLogoutServices().addAll(buildSingleLogoutServices());
        }
        idpssoDescriptor.getKeyDescriptors().addAll(buildKeyDescriptors());

        final EntityDescriptor descriptor = new EntityDescriptorBuilder().buildObject();
        descriptor.setEntityID(this.entityId);
        descriptor.getRoleDescriptors().add(idpssoDescriptor);
        if (organization != null) {
            descriptor.setOrganization(buildOrganization());
        }
        for (ContactInfo contact : contacts) {
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
        List<NameIDFormat> formats = new ArrayList();
        if (nameIdFormats.isEmpty()) {
            throw new IllegalStateException("NameID format has not been specified");
        }
        for (String nameIdFormat : nameIdFormats) {
            final NameIDFormat format = new NameIDFormatBuilder().buildObject();
            format.setFormat(nameIdFormat);
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
        url.setValue(this.organization.url);
        url.setXMLLang(lang);

        final Organization org = new OrganizationBuilder().buildObject();
        org.getOrganizationNames().add(name);
        org.getDisplayNames().add(displayName);
        org.getURLs().add(url);
        return org;
    }

    private ContactPerson buildContact(ContactInfo contact) {
        final GivenName givenName = new GivenNameBuilder().buildObject();
        givenName.setName(contact.givenName);
        final SurName surName = new SurNameBuilder().buildObject();
        surName.setName(contact.surName);
        final EmailAddress email = new EmailAddressBuilder().buildObject();
        email.setAddress(contact.email);

        final ContactPerson person = new ContactPersonBuilder().buildObject();
        person.setType(contact.type);
        person.setGivenName(givenName);
        person.setSurName(surName);
        person.getEmailAddresses().add(email);
        return person;
    }


    public static class ContactInfo {
        static final Map<String, ContactPersonTypeEnumeration> TYPES =
            MapBuilder.<String, ContactPersonTypeEnumeration>newMapBuilder(new LinkedHashMap<>())
                .put(ContactPersonTypeEnumeration.ADMINISTRATIVE.toString(), ContactPersonTypeEnumeration.ADMINISTRATIVE)
                .put(ContactPersonTypeEnumeration.BILLING.toString(), ContactPersonTypeEnumeration.BILLING)
                .put(ContactPersonTypeEnumeration.SUPPORT.toString(), ContactPersonTypeEnumeration.SUPPORT)
                .put(ContactPersonTypeEnumeration.TECHNICAL.toString(), ContactPersonTypeEnumeration.TECHNICAL)
                .put(ContactPersonTypeEnumeration.OTHER.toString(), ContactPersonTypeEnumeration.OTHER)
                .map();

        public final ContactPersonTypeEnumeration type;
        public final String givenName;
        public final String surName;
        public final String email;

        public ContactInfo(ContactPersonTypeEnumeration type, String givenName, String surName, String email) {
            this.type = Objects.requireNonNull(type, "Contact Person Type is required");
            this.givenName = givenName;
            this.surName = surName;
            this.email = Objects.requireNonNull(email, "Contact Person email is required");
        }

        private static ContactPersonTypeEnumeration getType(String name) {
            final ContactPersonTypeEnumeration type = TYPES.get(name.toLowerCase(Locale.ROOT));
            if (type == null) {
                throw new IllegalArgumentException("Invalid contact type " + name + " allowed values are "
                    + Strings.collectionToCommaDelimitedString(TYPES.keySet()));
            }
            return type;
        }
    }

    public static class OrganizationInfo {
        public final String organizationName;
        public final String displayName;
        public final String url;

        public OrganizationInfo(String organizationName, String displayName, String url) {
            this.organizationName = organizationName;
            this.displayName = displayName;
            this.url = url;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrganizationInfo that = (OrganizationInfo) o;
            return Objects.equals(organizationName, that.organizationName) &&
                Objects.equals(displayName, that.displayName) &&
                Objects.equals(url, that.url);
        }

        @Override
        public int hashCode() {
            return Objects.hash(organizationName, displayName, url);
        }
    }
}
