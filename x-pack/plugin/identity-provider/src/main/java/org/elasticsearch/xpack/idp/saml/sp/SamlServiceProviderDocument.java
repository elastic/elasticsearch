/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.joda.time.Duration;
import org.joda.time.ReadableDuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * This class models the storage of a {@link SamlServiceProvider} as an Elasticsearch document.
 */
public class SamlServiceProviderDocument implements ToXContentObject {

    public static final String SIGN_AUTHN = "authn";
    public static final String SIGN_LOGOUT = "logout";
    private static final Set<String> ALLOWED_SIGN_MESSAGES = Set.of(SIGN_AUTHN, SIGN_LOGOUT);

    public static class Privileges {
        @Nullable
        public String application;
        public String resource;
        @Nullable
        public String loginAction;
        public Map<String, String> groupActions;

        public void setApplication(String application) {
            this.application = application;
        }

        public void setResource(String resource) {
            this.resource = resource;
        }

        public void setLoginAction(String loginAction) {
            this.loginAction = loginAction;
        }

        public void setGroupActions(Map<String, String> groupActions) {
            this.groupActions = groupActions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Privileges that = (Privileges) o;
            return Objects.equals(application, that.application) &&
                Objects.equals(resource, that.resource) &&
                Objects.equals(loginAction, that.loginAction) &&
                Objects.equals(groupActions, that.groupActions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(application, resource, loginAction, groupActions);
        }
    }

    public static class AttributeNames {
        public String principal;
        @Nullable
        public String email;
        @Nullable
        public String name;
        @Nullable
        public String groups;

        public void setPrincipal(String principal) {
            this.principal = principal;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setGroups(String groups) {
            this.groups = groups;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final AttributeNames that = (AttributeNames) o;
            return Objects.equals(principal, that.principal) &&
                Objects.equals(email, that.email) &&
                Objects.equals(name, that.name) &&
                Objects.equals(groups, that.groups);
        }

        @Override
        public int hashCode() {
            return Objects.hash(principal, email, name, groups);
        }
    }

    public static class Certificates {
        public List<String> serviceProviderSigning = List.of();
        public List<String> identityProviderSigning = List.of();
        public List<String> identityProviderMetadataSigning = List.of();

        public void setServiceProviderSigning(Collection<String> serviceProviderSigning) {
            this.serviceProviderSigning = serviceProviderSigning == null ? List.of() : List.copyOf(serviceProviderSigning);
        }

        public void setIdentityProviderSigning(Collection<String> identityProviderSigning) {
            this.identityProviderSigning = identityProviderSigning == null ? List.of() : List.copyOf(identityProviderSigning);
        }

        public void setIdentityProviderMetadataSigning(Collection<String> identityProviderMetadataSigning) {
            this.identityProviderMetadataSigning
                = identityProviderMetadataSigning == null ? List.of() : List.copyOf(identityProviderMetadataSigning);
        }

        public void setServiceProviderX509SigningCertificates(Collection<X509Certificate> certificates) {
            this.serviceProviderSigning = encodeCertificates(certificates);
        }

        public List<X509Certificate> getServiceProviderX509SigningCertificates() {
            return decodeCertificates(this.serviceProviderSigning);
        }

        public void setIdentityProviderX509SigningCertificates(Collection<X509Certificate> certificates) {
            this.identityProviderSigning = encodeCertificates(certificates);
        }

        public List<X509Certificate> getIdentityProviderX509SigningCertificates() {
            return decodeCertificates(this.identityProviderSigning);
        }

        public void setIdentityProviderX509MetadataSigningCertificates(Collection<X509Certificate> certificates) {
            this.identityProviderMetadataSigning = encodeCertificates(certificates);
        }

        public List<X509Certificate> getIdentityProviderX509MetadataSigningCertificates() {
            return decodeCertificates(this.identityProviderMetadataSigning);
        }

        private List<String> encodeCertificates(Collection<X509Certificate> certificates) {
            return certificates == null ? List.of() : certificates.stream()
                .map(cert -> {
                    try {
                        return cert.getEncoded();
                    } catch (CertificateEncodingException e) {
                        throw new ElasticsearchException("Cannot read certificate", e);
                    }
                })
                .map(Base64.getEncoder()::encodeToString)
                .collect(Collectors.toUnmodifiableList());
        }

        private List<X509Certificate> decodeCertificates(List<String> encodedCertificates) {
            if (encodedCertificates == null || encodedCertificates.isEmpty()) {
                return List.of();
            }
            return encodedCertificates.stream().map(this::decodeCertificate).collect(Collectors.toUnmodifiableList());
        }

        private X509Certificate decodeCertificate(String base64Cert) {
            final byte[] bytes = base64Cert.getBytes(StandardCharsets.UTF_8);
            try (InputStream stream = new ByteArrayInputStream(bytes)) {
                final List<Certificate> certificates = CertParsingUtils.readCertificates(Base64.getDecoder().wrap(stream));
                if (certificates.size() == 1) {
                    final Certificate certificate = certificates.get(0);
                    if (certificate instanceof X509Certificate) {
                        return (X509Certificate) certificate;
                    } else {
                        throw new ElasticsearchException("Certificate ({}) is not a X.509 certificate", certificate.getClass());
                    }
                } else {
                    throw new ElasticsearchException("Expected a single certificate, but found {}", certificates.size());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (CertificateException e) {
                throw new ElasticsearchException("Cannot parse certificate(s)", e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Certificates that = (Certificates) o;
            return Objects.equals(serviceProviderSigning, that.serviceProviderSigning) &&
                Objects.equals(identityProviderSigning, that.identityProviderSigning) &&
                Objects.equals(identityProviderMetadataSigning, that.identityProviderMetadataSigning);
        }

        @Override
        public int hashCode() {
            return Objects.hash(serviceProviderSigning, identityProviderSigning, identityProviderMetadataSigning);
        }
    }

    public String docId;

    public String name;

    public String entityId;

    public String acs;

    public boolean enabled = true;
    public Instant created;
    public Instant lastModified;

    public Set<String> nameIdFormats = Set.of();
    public Set<String> signMessages = Set.of();

    @Nullable
    public Long authenticationExpiryMillis;

    public final Privileges privileges = new Privileges();
    public final AttributeNames attributeNames = new AttributeNames();
    public final Certificates certificates = new Certificates();

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public void setAcs(String acs) {
        this.acs = acs;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setCreatedMillis(Long millis) {
        this.created = Instant.ofEpochMilli(millis);
    }

    public void setLastModifiedMillis(Long millis) {
        this.lastModified = Instant.ofEpochMilli(millis);
    }

    public void setNameIdFormats(Collection<String> nameIdFormats) {
        this.nameIdFormats = nameIdFormats == null ? Set.of() : Set.copyOf(nameIdFormats);
    }

    public void setSignMessages(Collection<String> signMessages) {
        this.signMessages = signMessages == null ? Set.of() : Set.copyOf(signMessages);
    }

    public void setAuthenticationExpiryMillis(Long authenticationExpiryMillis) {
        this.authenticationExpiryMillis = authenticationExpiryMillis;
    }

    public void setAuthenticationExpiry(ReadableDuration authnExpiry) {
        this.authenticationExpiryMillis = authnExpiry == null ? null : authnExpiry.getMillis();
    }

    public ReadableDuration getAuthenticationExpiry() {
        return authenticationExpiryMillis == null ? null : Duration.millis(this.authenticationExpiryMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SamlServiceProviderDocument that = (SamlServiceProviderDocument) o;
        return Objects.equals(docId, that.docId) &&
            Objects.equals(name, that.name) &&
            Objects.equals(entityId, that.entityId) &&
            Objects.equals(acs, that.acs) &&
            Objects.equals(enabled, that.enabled) &&
            Objects.equals(created, that.created) &&
            Objects.equals(lastModified, that.lastModified) &&
            Objects.equals(nameIdFormats, that.nameIdFormats) &&
            Objects.equals(authenticationExpiryMillis, that.authenticationExpiryMillis) &&
            Objects.equals(certificates, that.certificates) &&
            Objects.equals(privileges, that.privileges) &&
            Objects.equals(attributeNames, that.attributeNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docId, name, entityId, acs, enabled, created, lastModified, nameIdFormats, authenticationExpiryMillis,
            certificates, privileges, attributeNames);
    }

    private static final ObjectParser<SamlServiceProviderDocument, SamlServiceProviderDocument> DOC_PARSER
        = new ObjectParser<>("service_provider_doc", true, SamlServiceProviderDocument::new);
    private static final ObjectParser<Privileges, Void> PRIVILEGES_PARSER = new ObjectParser<>("service_provider_priv", true, null);
    private static final ObjectParser<AttributeNames, Void> ATTRIBUTES_PARSER = new ObjectParser<>("service_provider_attr", true, null);
    private static final ObjectParser<Certificates, Void> CERTIFICATES_PARSER = new ObjectParser<>("service_provider_cert", true, null);

    private static final BiConsumer<SamlServiceProviderDocument, Object> NULL_CONSUMER = (doc, obj) -> {
    };

    static {
        DOC_PARSER.declareString(SamlServiceProviderDocument::setName, Fields.NAME);
        DOC_PARSER.declareString(SamlServiceProviderDocument::setEntityId, Fields.ENTITY_ID);
        DOC_PARSER.declareString(SamlServiceProviderDocument::setAcs, Fields.ACS);
        DOC_PARSER.declareBoolean(SamlServiceProviderDocument::setEnabled, Fields.ENABLED);
        DOC_PARSER.declareLong(SamlServiceProviderDocument::setCreatedMillis, Fields.CREATED_DATE);
        DOC_PARSER.declareLong(SamlServiceProviderDocument::setLastModifiedMillis, Fields.LAST_MODIFIED);
        DOC_PARSER.declareStringArray(SamlServiceProviderDocument::setNameIdFormats, Fields.NAME_ID);
        DOC_PARSER.declareStringArray(SamlServiceProviderDocument::setSignMessages, Fields.SIGN_MSGS);
        DOC_PARSER.declareField(SamlServiceProviderDocument::setAuthenticationExpiryMillis,
            parser -> parser.currentToken() == XContentParser.Token.VALUE_NULL ? null : parser.longValue(),
            Fields.AUTHN_EXPIRY, ObjectParser.ValueType.LONG_OR_NULL);

        DOC_PARSER.declareObject(NULL_CONSUMER, (parser, doc) -> PRIVILEGES_PARSER.parse(parser, doc.privileges, null), Fields.PRIVILEGES);
        PRIVILEGES_PARSER.declareStringOrNull(Privileges::setApplication, Fields.Privileges.APPLICATION);
        PRIVILEGES_PARSER.declareString(Privileges::setResource, Fields.Privileges.RESOURCE);
        PRIVILEGES_PARSER.declareStringOrNull(Privileges::setLoginAction, Fields.Privileges.LOGIN_ACTION);
        PRIVILEGES_PARSER.declareField(Privileges::setGroupActions,
            (parser, ignore) -> parser.currentToken() == XContentParser.Token.VALUE_NULL ? null : parser.mapStrings(),
            Fields.Privileges.GROUPS, ObjectParser.ValueType.OBJECT_OR_NULL);

        DOC_PARSER.declareObject(NULL_CONSUMER, (p, doc) -> ATTRIBUTES_PARSER.parse(p, doc.attributeNames, null), Fields.ATTRIBUTES);
        ATTRIBUTES_PARSER.declareString(AttributeNames::setPrincipal, Fields.Attributes.PRINCIPAL);
        ATTRIBUTES_PARSER.declareStringOrNull(AttributeNames::setEmail, Fields.Attributes.EMAIL);
        ATTRIBUTES_PARSER.declareStringOrNull(AttributeNames::setName, Fields.Attributes.NAME);
        ATTRIBUTES_PARSER.declareStringOrNull(AttributeNames::setGroups, Fields.Attributes.GROUPS);

        DOC_PARSER.declareObject(NULL_CONSUMER, (p, doc) -> CERTIFICATES_PARSER.parse(p, doc.certificates, null), Fields.CERTIFICATES);
        CERTIFICATES_PARSER.declareStringArray(Certificates::setServiceProviderSigning, Fields.Certificates.SP_SIGNING);
        CERTIFICATES_PARSER.declareStringArray(Certificates::setIdentityProviderSigning, Fields.Certificates.IDP_SIGNING);
        CERTIFICATES_PARSER.declareStringArray(Certificates::setIdentityProviderMetadataSigning, Fields.Certificates.IDP_METADATA);
    }

    public static SamlServiceProviderDocument fromXContent(String docId, XContentParser parser) throws IOException {
        SamlServiceProviderDocument doc = new SamlServiceProviderDocument();
        doc.setDocId(docId);
        return DOC_PARSER.parse(parser, doc, doc);
    }

    public ValidationException validate() {
        final ValidationException validation = new ValidationException();
        if (Strings.isNullOrEmpty(name)) {
            validation.addValidationError("field [" + Fields.NAME.getPreferredName() + "] is required, but was [" + name + "]");
        }
        if (Strings.isNullOrEmpty(entityId)) {
            validation.addValidationError("field [" + Fields.ENTITY_ID.getPreferredName() + "] is required, but was [" + entityId + "]");
        }
        if (Strings.isNullOrEmpty(acs)) {
            validation.addValidationError("field [" + Fields.ACS.getPreferredName() + "] is required, but was [" + acs + "]");
        }
        if (created == null) {
            validation.addValidationError("field [" + Fields.CREATED_DATE.getPreferredName() + "] is required, but was [" + created + "]");
        }
        if (lastModified == null) {
            validation.addValidationError(
                "field [" + Fields.LAST_MODIFIED.getPreferredName() + "] is required, but was [" + lastModified + "]");
        }

        final Set<String> invalidSignOptions = Sets.difference(signMessages, ALLOWED_SIGN_MESSAGES);
        if (invalidSignOptions.isEmpty() == false) {
            validation.addValidationError("the values [" + invalidSignOptions + "] are not permitted for [" + Fields.SIGN_MSGS
                + "] - permitted values are [" + ALLOWED_SIGN_MESSAGES + "]");
        }

        if (Strings.isNullOrEmpty(privileges.resource)) {
            validation.addValidationError("field [" + Fields.PRIVILEGES.getPreferredName() + "."
                + Fields.Privileges.RESOURCE.getPreferredName() + "] is required, but was [" + privileges.resource + "]");
        }
        if (Strings.isNullOrEmpty(attributeNames.principal)) {
            validation.addValidationError("field [" + Fields.ATTRIBUTES.getPreferredName() + "."
                + Fields.Attributes.PRINCIPAL.getPreferredName() + "] is required, but was [" + attributeNames.principal + "]");
        }
        if (validation.validationErrors().isEmpty()) {
            return null;
        } else {
            return validation;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.NAME.getPreferredName(), name);
        builder.field(Fields.ENTITY_ID.getPreferredName(), entityId);
        builder.field(Fields.ACS.getPreferredName(), acs);
        builder.field(Fields.ENABLED.getPreferredName(), enabled);
        builder.field(Fields.CREATED_DATE.getPreferredName(), created == null ? null : created.toEpochMilli());
        builder.field(Fields.LAST_MODIFIED.getPreferredName(), lastModified == null ? null : lastModified.toEpochMilli());
        builder.field(Fields.NAME_ID.getPreferredName(), nameIdFormats == null ? List.of() : nameIdFormats);
        builder.field(Fields.SIGN_MSGS.getPreferredName(), signMessages == null ? List.of() : signMessages);
        builder.field(Fields.AUTHN_EXPIRY.getPreferredName(), authenticationExpiryMillis);

        builder.startObject(Fields.PRIVILEGES.getPreferredName());
        builder.field(Fields.Privileges.APPLICATION.getPreferredName(), privileges.application);
        builder.field(Fields.Privileges.RESOURCE.getPreferredName(), privileges.resource);
        builder.field(Fields.Privileges.LOGIN_ACTION.getPreferredName(), privileges.loginAction);
        builder.field(Fields.Privileges.GROUPS.getPreferredName(), privileges.groupActions);
        builder.endObject();

        builder.startObject(Fields.ATTRIBUTES.getPreferredName());
        builder.field(Fields.Attributes.PRINCIPAL.getPreferredName(), attributeNames.principal);
        builder.field(Fields.Attributes.EMAIL.getPreferredName(), attributeNames.email);
        builder.field(Fields.Attributes.NAME.getPreferredName(), attributeNames.name);
        builder.field(Fields.Attributes.GROUPS.getPreferredName(), attributeNames.groups);
        builder.endObject();

        builder.startObject(Fields.CERTIFICATES.getPreferredName());
        builder.field(Fields.Certificates.SP_SIGNING.getPreferredName(), certificates.serviceProviderSigning);
        builder.field(Fields.Certificates.IDP_SIGNING.getPreferredName(), certificates.identityProviderSigning);
        builder.field(Fields.Certificates.IDP_METADATA.getPreferredName(), certificates.identityProviderMetadataSigning);
        builder.endObject();

        return builder.endObject();
    }

    interface Fields {
        ParseField NAME = new ParseField("name");
        ParseField ENTITY_ID = new ParseField("entity_id");
        ParseField ACS = new ParseField("acs");
        ParseField ENABLED = new ParseField("enabled");
        ParseField NAME_ID = new ParseField("name_id_format");
        ParseField SIGN_MSGS = new ParseField("sign_messages");
        ParseField AUTHN_EXPIRY = new ParseField("authn_expiry_ms");

        ParseField CREATED_DATE = new ParseField("created");
        ParseField LAST_MODIFIED = new ParseField("last_modified");

        ParseField PRIVILEGES = new ParseField("privileges");
        ParseField ATTRIBUTES = new ParseField("attributes");
        ParseField CERTIFICATES = new ParseField("certificates");

        interface Privileges {
            ParseField APPLICATION = new ParseField("application");
            ParseField RESOURCE = new ParseField("resource");
            ParseField LOGIN_ACTION = new ParseField("login");
            ParseField GROUPS = new ParseField("groups");
        }

        interface Attributes {
            ParseField PRINCIPAL = new ParseField("principal");
            ParseField EMAIL = new ParseField("email");
            ParseField NAME = new ParseField("name");
            ParseField GROUPS = new ParseField("groups");
        }

        interface Certificates {
            ParseField SP_SIGNING = new ParseField("sp_signing");
            ParseField IDP_SIGNING = new ParseField("idp_signing");
            ParseField IDP_METADATA = new ParseField("idp_metadata");
        }
    }
}
