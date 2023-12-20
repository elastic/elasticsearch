/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import net.shibboleth.utilities.java.support.collection.LockableClassToInstanceMultiMap;

import org.elasticsearch.core.Nullable;
import org.opensaml.core.xml.Namespace;
import org.opensaml.core.xml.NamespaceManager;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.schema.XSBooleanValue;
import org.opensaml.core.xml.util.AttributeMap;
import org.opensaml.core.xml.util.IDIndex;
import org.opensaml.saml.saml2.metadata.AdditionalMetadataLocation;
import org.opensaml.saml.saml2.metadata.AffiliationDescriptor;
import org.opensaml.saml.saml2.metadata.AttributeAuthorityDescriptor;
import org.opensaml.saml.saml2.metadata.AuthnAuthorityDescriptor;
import org.opensaml.saml.saml2.metadata.ContactPerson;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.Extensions;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.Organization;
import org.opensaml.saml.saml2.metadata.PDPDescriptor;
import org.opensaml.saml.saml2.metadata.RoleDescriptor;
import org.opensaml.saml.saml2.metadata.SPSSODescriptor;
import org.opensaml.xmlsec.signature.Signature;
import org.w3c.dom.Element;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import javax.xml.namespace.QName;

class UnresolvedEntity implements EntityDescriptor {

    private String entityId;
    private String sourceLocation;

    UnresolvedEntity(String entityId, String sourceLocation) {
        this.entityId = entityId;
        this.sourceLocation = sourceLocation;
    }

    @Override
    public String getEntityID() {
        return entityId;
    }

    @Override
    public String toString() {
        return getID();
    }

    @Override
    public void setEntityID(String id) {
        throw new UnsupportedOperationException("Cannot set Entity Id of " + this);
    }

    @Override
    public String getID() {
        return "Unresolved-SAML-Entity{" + entityId + '}';
    }

    @Override
    public void setID(String newID) {
        throw new UnsupportedOperationException("Cannot set Element ID of " + this);
    }

    @Override
    public Extensions getExtensions() {
        return null;
    }

    @Override
    public void setExtensions(Extensions extensions) {
        throw new UnsupportedOperationException("Cannot set extensions on " + this);
    }

    @Override
    public List<RoleDescriptor> getRoleDescriptors() {
        throw SamlUtils.samlException(
            "Cannot get role descriptors" + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            sourceLocation,
            entityId
        );
    }

    @Override
    public List<RoleDescriptor> getRoleDescriptors(QName typeOrName) {
        throw SamlUtils.samlException(
            "Cannot get role descriptors [type/name={}]"
                + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            typeOrName,
            sourceLocation,
            entityId
        );
    }

    @Override
    public List<RoleDescriptor> getRoleDescriptors(QName typeOrName, String supportedProtocol) {
        throw SamlUtils.samlException(
            "Cannot get role descriptors [type/name={}][protocol={}]"
                + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            typeOrName,
            supportedProtocol,
            sourceLocation,
            entityId
        );
    }

    @Override
    public IDPSSODescriptor getIDPSSODescriptor(String supportedProtocol) {
        throw SamlUtils.samlException(
            "Cannot get IdP SSO descriptor [protocol={}]"
                + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            supportedProtocol,
            sourceLocation,
            entityId
        );
    }

    @Override
    public SPSSODescriptor getSPSSODescriptor(String supportedProtocol) {
        throw SamlUtils.samlException(
            "Cannot get SP SSO descriptor [protocol={}]"
                + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            supportedProtocol,
            sourceLocation,
            entityId
        );
    }

    @Override
    public AuthnAuthorityDescriptor getAuthnAuthorityDescriptor(String supportedProtocol) {
        throw SamlUtils.samlException(
            "Cannot get authn authority descriptor [protocol={}]"
                + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            supportedProtocol,
            sourceLocation,
            entityId
        );
    }

    @Override
    public AttributeAuthorityDescriptor getAttributeAuthorityDescriptor(String supportedProtocol) {
        throw SamlUtils.samlException(
            "Cannot get attribute authority descriptor [protocol={}]"
                + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            supportedProtocol,
            sourceLocation,
            entityId
        );
    }

    @Override
    public PDPDescriptor getPDPDescriptor(String supportedProtocol) {
        throw SamlUtils.samlException(
            "Cannot get PDP descriptor [protocol={}]" + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            supportedProtocol,
            sourceLocation,
            entityId
        );
    }

    @Override
    public AffiliationDescriptor getAffiliationDescriptor() {
        throw SamlUtils.samlException(
            "Cannot get affiliation descriptor" + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            sourceLocation,
            entityId
        );
    }

    @Override
    public void setAffiliationDescriptor(AffiliationDescriptor descriptor) {
        throw new UnsupportedOperationException("Cannot set affiliation descriptor of " + this);
    }

    @Override
    public Organization getOrganization() {
        throw SamlUtils.samlException(
            "Cannot get organization" + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            sourceLocation,
            entityId
        );
    }

    @Override
    public void setOrganization(Organization organization) {
        throw new UnsupportedOperationException("Cannot set organization of " + this);
    }

    @Override
    public List<ContactPerson> getContactPersons() {
        return List.of();
    }

    @Override
    public List<AdditionalMetadataLocation> getAdditionalMetadataLocations() {
        return List.of();
    }

    @Override
    public AttributeMap getUnknownAttributes() {
        return null;
    }

    @Override
    public String getSignatureReferenceID() {
        throw SamlUtils.samlException(
            "Cannot get signature reference id" + " because the metadata [location={}] for SAML entity [id={}] could not be resolved",
            sourceLocation,
            entityId
        );
    }

    @Nullable
    @Override
    public Duration getCacheDuration() {
        return null;
    }

    @Override
    public void setCacheDuration(Duration duration) {
        throw new UnsupportedOperationException("Cannot set cache duration of " + this);
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Nullable
    @Override
    public Instant getValidUntil() {
        return null;
    }

    @Override
    public void setValidUntil(Instant validUntil) {
        throw new UnsupportedOperationException("Cannot set valid-until of " + this);
    }

    @Override
    public boolean isSigned() {
        return false;
    }

    @Nullable
    @Override
    public Signature getSignature() {
        return null;
    }

    @Override
    public void setSignature(Signature newSignature) {
        throw new UnsupportedOperationException("Cannot set signature of " + this);
    }

    @Override
    public void detach() {}

    @Nullable
    @Override
    public Element getDOM() {
        return null;
    }

    @Override
    public QName getElementQName() {
        return EntityDescriptor.ELEMENT_QNAME;
    }

    @Override
    public IDIndex getIDIndex() {
        throw new UnsupportedOperationException("Cannot get ID Index of " + this);
    }

    @Override
    public NamespaceManager getNamespaceManager() {
        throw new UnsupportedOperationException("Cannot get namespace manager of " + this);
    }

    @Override
    public Set<Namespace> getNamespaces() {
        throw new UnsupportedOperationException("Cannot get namespaces of " + this);
    }

    @Nullable
    @Override
    public String getNoNamespaceSchemaLocation() {
        return null;
    }

    @Nullable
    @Override
    public List<XMLObject> getOrderedChildren() {
        return List.of();
    }

    @Nullable
    @Override
    public XMLObject getParent() {
        return null;
    }

    @Nullable
    @Override
    public String getSchemaLocation() {
        return null;
    }

    @Nullable
    @Override
    public QName getSchemaType() {
        return null;
    }

    @Override
    public boolean hasChildren() {
        return false;
    }

    @Override
    public boolean hasParent() {
        return false;
    }

    @Override
    public void releaseChildrenDOM(boolean propagateRelease) {

    }

    @Override
    public void releaseDOM() {

    }

    @Override
    public void releaseParentDOM(boolean propagateRelease) {

    }

    @Nullable
    @Override
    public XMLObject resolveID(String id) {
        return null;
    }

    @Nullable
    @Override
    public XMLObject resolveIDFromRoot(String id) {
        return null;
    }

    @Override
    public void setDOM(Element dom) {
        throw new UnsupportedOperationException("Cannot set DOM of " + this);
    }

    @Override
    public void setNoNamespaceSchemaLocation(String location) {
        throw new UnsupportedOperationException("Cannot set no-namespace-schema-location of " + this);
    }

    @Override
    public void setParent(XMLObject parent) {
        throw new UnsupportedOperationException("Cannot set parent of " + this);
    }

    @Override
    public void setSchemaLocation(String location) {
        throw new UnsupportedOperationException("Cannot set schema location of " + this);
    }

    @Nullable
    @Override
    public Boolean isNil() {
        return null;
    }

    @Nullable
    @Override
    public XSBooleanValue isNilXSBoolean() {
        return null;
    }

    @Override
    public void setNil(Boolean newNil) {
        throw new UnsupportedOperationException("Cannot set NIL of " + this);
    }

    @Override
    public void setNil(XSBooleanValue newNil) {
        throw new UnsupportedOperationException("Cannot set NIL of " + this);
    }

    @Override
    public LockableClassToInstanceMultiMap<Object> getObjectMetadata() {
        throw new UnsupportedOperationException("Cannot get object-metadata of " + this);
    }
}
