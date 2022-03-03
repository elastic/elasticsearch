/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.metadata.AssertionConsumerService;
import org.opensaml.saml.saml2.metadata.AttributeConsumingService;
import org.opensaml.saml.saml2.metadata.ContactPerson;
import org.opensaml.saml.saml2.metadata.ContactPersonTypeEnumeration;
import org.opensaml.saml.saml2.metadata.EmailAddress;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.GivenName;
import org.opensaml.saml.saml2.metadata.KeyDescriptor;
import org.opensaml.saml.saml2.metadata.NameIDFormat;
import org.opensaml.saml.saml2.metadata.Organization;
import org.opensaml.saml.saml2.metadata.OrganizationDisplayName;
import org.opensaml.saml.saml2.metadata.OrganizationName;
import org.opensaml.saml.saml2.metadata.OrganizationURL;
import org.opensaml.saml.saml2.metadata.RequestedAttribute;
import org.opensaml.saml.saml2.metadata.SPSSODescriptor;
import org.opensaml.saml.saml2.metadata.ServiceName;
import org.opensaml.saml.saml2.metadata.SingleLogoutService;
import org.opensaml.saml.saml2.metadata.SurName;
import org.opensaml.saml.saml2.metadata.impl.AssertionConsumerServiceBuilder;
import org.opensaml.saml.saml2.metadata.impl.AttributeConsumingServiceBuilder;
import org.opensaml.saml.saml2.metadata.impl.ContactPersonBuilder;
import org.opensaml.saml.saml2.metadata.impl.EmailAddressBuilder;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorBuilder;
import org.opensaml.saml.saml2.metadata.impl.GivenNameBuilder;
import org.opensaml.saml.saml2.metadata.impl.KeyDescriptorBuilder;
import org.opensaml.saml.saml2.metadata.impl.NameIDFormatBuilder;
import org.opensaml.saml.saml2.metadata.impl.OrganizationBuilder;
import org.opensaml.saml.saml2.metadata.impl.OrganizationDisplayNameBuilder;
import org.opensaml.saml.saml2.metadata.impl.OrganizationNameBuilder;
import org.opensaml.saml.saml2.metadata.impl.OrganizationURLBuilder;
import org.opensaml.saml.saml2.metadata.impl.RequestedAttributeBuilder;
import org.opensaml.saml.saml2.metadata.impl.SPSSODescriptorBuilder;
import org.opensaml.saml.saml2.metadata.impl.ServiceNameBuilder;
import org.opensaml.saml.saml2.metadata.impl.SingleLogoutServiceBuilder;
import org.opensaml.saml.saml2.metadata.impl.SurNameBuilder;
import org.opensaml.security.credential.UsageType;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.keyinfo.KeyInfoSupport;
import org.opensaml.xmlsec.signature.KeyInfo;
import org.opensaml.xmlsec.signature.impl.KeyInfoBuilder;

import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Constructs SAML Metadata to describe a <em>Service Provider</em>.
 * This metadata is used to configure Identity Providers that will interact with the Service Provider.
 */
public class SamlSpMetadataBuilder {

    private final Locale locale;
    private final String entityId;
    private final Map<String, String> attributeNames;
    private final List<ContactInfo> contacts;

    private String serviceName;
    private String nameIdFormat;
    private String assertionConsumerServiceUrl;
    private String singleLogoutServiceUrl;
    private Boolean authnRequestsSigned;
    private X509Certificate signingCertificate;
    private List<X509Certificate> encryptionCertificates = new ArrayList<>();
    private OrganizationInfo organization;

    /**
     * @param locale   The locale to use for element that require {@code xml:lang} attributes
     * @param entityId The URI for the Service Provider entity
     */
    public SamlSpMetadataBuilder(Locale locale, String entityId) {
        this.locale = locale;
        this.entityId = entityId;
        this.attributeNames = new LinkedHashMap<>();
        this.contacts = new ArrayList<>();
        this.serviceName = "Elasticsearch";
        this.nameIdFormat = null;
        this.authnRequestsSigned = Boolean.FALSE;
    }

    /**
     * The format that the service provider expects for incoming NameID element.
     */
    public SamlSpMetadataBuilder nameIdFormat(String nameIdFormat) {
        this.nameIdFormat = nameIdFormat;
        return this;
    }

    /**
     * The name of the service, for use in a {@link AttributeConsumingService}
     */
    public SamlSpMetadataBuilder serviceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    /**
     * Request a named attribute be provided as part of assertions. Specified  in a {@link AttributeConsumingService}
     */
    public SamlSpMetadataBuilder withAttribute(String friendlyName, String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Attribute name cannot be empty (friendly name was [" + friendlyName + "])");
        }
        this.attributeNames.put(name, friendlyName);
        return this;
    }

    /**
     * The (POST) URL to be used to accept SAML assertions (authentication results)
     */
    public SamlSpMetadataBuilder assertionConsumerServiceUrl(String acsUrl) {
        this.assertionConsumerServiceUrl = acsUrl;
        return this;
    }

    /**
     * The (GET/Redirect) URL to be used to handle SAML logout / session termination
     */
    public SamlSpMetadataBuilder singleLogoutServiceUrl(String slsUrl) {
        this.singleLogoutServiceUrl = slsUrl;
        return this;
    }

    /**
     * Whether this Service Provider signs {@link org.opensaml.saml.saml2.core.AuthnRequest} messages.
     */
    public SamlSpMetadataBuilder authnRequestsSigned(Boolean authnRequestsSigned) {
        this.authnRequestsSigned = authnRequestsSigned;
        return this;
    }

    /**
     * The certificate that the service provider users to sign SAML requests.
     */
    public SamlSpMetadataBuilder signingCertificate(X509Certificate signingCertificate) {
        this.signingCertificate = signingCertificate;
        return this;
    }

    /**
     * The certificate credential that should be used to send encrypted data to the service provider.
     */
    public SamlSpMetadataBuilder signingCredential(X509Credential credential) {
        return signingCertificate(credential == null ? null : credential.getEntityCertificate());
    }

    /**
     * The certificate that should be used to send encrypted data to the service provider.
     */
    public SamlSpMetadataBuilder encryptionCertificates(Collection<X509Certificate> encryptionCertificates) {
        if (encryptionCertificates != null) {
            this.encryptionCertificates.addAll(encryptionCertificates);
        }
        return this;
    }

    /**
     * The certificate credential that should be used to send encrypted data to the service provider.
     */
    public SamlSpMetadataBuilder encryptionCredentials(Collection<X509Credential> credentials) {
        return encryptionCertificates(
            credentials == null
                ? Collections.emptyList()
                : credentials.stream().map(credential -> credential.getEntityCertificate()).collect(Collectors.toList())
        );
    }

    /**
     * The organisation that operates the service provider
     */
    public SamlSpMetadataBuilder organization(OrganizationInfo organization) {
        this.organization = organization;
        return this;
    }

    /**
     * The organisation that operates the service provider
     */
    public SamlSpMetadataBuilder organization(String orgName, String displayName, String url) {
        return organization(new OrganizationInfo(orgName, displayName, url));
    }

    /**
     * A contact within the organisation that operates the service provider
     */
    public SamlSpMetadataBuilder withContact(ContactInfo contact) {
        this.contacts.add(contact);
        return this;
    }

    /**
     * A contact within the organisation that operates the service provider
     *
     * @param type Must be one of the standard types on {@link ContactPersonTypeEnumeration}
     */
    public SamlSpMetadataBuilder withContact(String type, String givenName, String surName, String email) {
        return withContact(new ContactInfo(ContactInfo.getType(type), givenName, surName, email));
    }

    /**
     * Constructs an {@link EntityDescriptor} that contains a single {@link SPSSODescriptor}.
     */
    public EntityDescriptor build() throws Exception {
        final SPSSODescriptor spRoleDescriptor = new SPSSODescriptorBuilder().buildObject();
        spRoleDescriptor.removeAllSupportedProtocols();
        spRoleDescriptor.addSupportedProtocol(SAMLConstants.SAML20P_NS);
        spRoleDescriptor.setWantAssertionsSigned(true);
        spRoleDescriptor.setAuthnRequestsSigned(this.authnRequestsSigned);

        if (Strings.isNullOrEmpty(nameIdFormat) == false) {
            spRoleDescriptor.getNameIDFormats().add(buildNameIdFormat());
        }
        spRoleDescriptor.getAssertionConsumerServices().add(buildAssertionConsumerService());
        if (attributeNames.size() > 0) {
            spRoleDescriptor.getAttributeConsumingServices().add(buildAttributeConsumerService());
        }
        if (Strings.hasText(singleLogoutServiceUrl)) {
            spRoleDescriptor.getSingleLogoutServices().add(buildSingleLogoutService());
        }

        spRoleDescriptor.getKeyDescriptors().addAll(buildKeyDescriptors());

        final EntityDescriptor descriptor = new EntityDescriptorBuilder().buildObject();
        descriptor.setEntityID(this.entityId);
        descriptor.getRoleDescriptors().add(spRoleDescriptor);
        if (organization != null) {
            descriptor.setOrganization(buildOrganization());
        }
        if (contacts.size() > 0) {
            contacts.forEach(c -> descriptor.getContactPersons().add(buildContact(c)));
        }

        return descriptor;
    }

    private NameIDFormat buildNameIdFormat() {
        if (Strings.isNullOrEmpty(nameIdFormat)) {
            throw new IllegalStateException("NameID format has not been specified");
        }
        final NameIDFormat format = new NameIDFormatBuilder().buildObject();
        format.setURI(this.nameIdFormat);
        return format;
    }

    private AssertionConsumerService buildAssertionConsumerService() {
        if (Strings.isNullOrEmpty(assertionConsumerServiceUrl)) {
            throw new IllegalStateException("AssertionConsumerService URL has not been specified");
        }
        final AssertionConsumerService acs = new AssertionConsumerServiceBuilder().buildObject();
        acs.setBinding(SAMLConstants.SAML2_POST_BINDING_URI);
        acs.setIndex(1);
        acs.setIsDefault(Boolean.TRUE);
        acs.setLocation(assertionConsumerServiceUrl);
        return acs;
    }

    private AttributeConsumingService buildAttributeConsumerService() {
        final AttributeConsumingService service = new AttributeConsumingServiceBuilder().buildObject();
        service.setIndex(1);
        service.setIsDefault(true);
        service.getNames().add(buildServiceName());
        attributeNames.forEach(
            (name, friendlyName) -> { service.getRequestedAttributes().add(buildRequestedAttribute(friendlyName, name)); }
        );
        return service;
    }

    private ServiceName buildServiceName() {
        final ServiceName name = new ServiceNameBuilder().buildObject();
        name.setValue(serviceName);
        name.setXMLLang(locale.toLanguageTag());
        return name;
    }

    private RequestedAttribute buildRequestedAttribute(String friendlyName, String name) {
        final RequestedAttribute attribute = new RequestedAttributeBuilder().buildObject();
        if (Strings.hasText(friendlyName)) {
            attribute.setFriendlyName(friendlyName);
        }
        attribute.setName(name);
        attribute.setNameFormat(RequestedAttribute.URI_REFERENCE);
        return attribute;
    }

    private SingleLogoutService buildSingleLogoutService() {
        final SingleLogoutService service = new SingleLogoutServiceBuilder().buildObject();
        // The draft Interoperable SAML 2 profile requires redirect binding.
        // That's annoying, because they require POST binding for the ACS so now SPs need to
        // support 2 bindings that have different signature passing rules, etc. *sigh*
        service.setBinding(SAMLConstants.SAML2_REDIRECT_BINDING_URI);
        service.setLocation(singleLogoutServiceUrl);
        return service;
    }

    private List<? extends KeyDescriptor> buildKeyDescriptors() throws CertificateEncodingException {
        if (encryptionCertificates.isEmpty() && signingCertificate == null) {
            return Collections.emptyList();
        }
        if (encryptionCertificates.size() == 1 && Objects.equals(encryptionCertificates.get(0), signingCertificate)) {
            return Collections.singletonList(buildKeyDescriptor(encryptionCertificates.get(0), UsageType.UNSPECIFIED));
        }
        List<KeyDescriptor> keys = new ArrayList<>();
        if (signingCertificate != null) {
            keys.add(buildKeyDescriptor(signingCertificate, UsageType.SIGNING));
        }
        for (X509Certificate encryptionCertificate : encryptionCertificates) {
            keys.add(buildKeyDescriptor(encryptionCertificate, UsageType.ENCRYPTION));
        }
        return keys;
    }

    private KeyDescriptor buildKeyDescriptor(X509Certificate certificate, UsageType usageType) throws CertificateEncodingException {
        final KeyDescriptor descriptor = new KeyDescriptorBuilder().buildObject();
        descriptor.setUse(usageType);
        final KeyInfo keyInfo = new KeyInfoBuilder().buildObject();
        KeyInfoSupport.addCertificate(keyInfo, certificate);
        descriptor.setKeyInfo(keyInfo);
        return descriptor;
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

    private ContactPerson buildContact(ContactInfo contact) {
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

    public static class OrganizationInfo {
        public final String organizationName;
        public final String displayName;
        public final String url;

        public OrganizationInfo(String organizationName, String displayName, String url) {
            if (Strings.isNullOrEmpty(organizationName)) {
                throw new IllegalArgumentException("Organization Name is required");
            }
            if (Strings.isNullOrEmpty(displayName)) {
                throw new IllegalArgumentException("Organization Display Name is required");
            }
            if (Strings.isNullOrEmpty(url)) {
                throw new IllegalArgumentException("Organization URL is required");
            }
            this.organizationName = organizationName;
            this.displayName = displayName;
            this.url = url;
        }
    }

    public static class ContactInfo {
        static final Map<String, ContactPersonTypeEnumeration> TYPES = MapBuilder.<String, ContactPersonTypeEnumeration>newMapBuilder(
            new LinkedHashMap<>()
        )
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
            this.email = email;
        }

        private static ContactPersonTypeEnumeration getType(String name) {
            final ContactPersonTypeEnumeration type = TYPES.get(name.toLowerCase(Locale.ROOT));
            if (type == null) {
                throw new IllegalArgumentException(
                    "Invalid contact type " + name + " allowed values are " + Strings.collectionToCommaDelimitedString(TYPES.keySet())
                );
            }
            return type;
        }
    }

}
