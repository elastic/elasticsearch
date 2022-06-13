/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.Attribute;
import org.opensaml.saml.saml2.core.AttributeStatement;
import org.opensaml.saml.saml2.core.Audience;
import org.opensaml.saml.saml2.core.AudienceRestriction;
import org.opensaml.saml.saml2.core.AuthnStatement;
import org.opensaml.saml.saml2.core.Conditions;
import org.opensaml.saml.saml2.core.EncryptedAssertion;
import org.opensaml.saml.saml2.core.EncryptedAttribute;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.Subject;
import org.opensaml.saml.saml2.core.SubjectConfirmation;
import org.opensaml.saml.saml2.core.SubjectConfirmationData;
import org.opensaml.xmlsec.encryption.support.DecryptionException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.security.authc.saml.SamlAttributes.NAMEID_SYNTHENTIC_ATTRIBUTE;
import static org.elasticsearch.xpack.security.authc.saml.SamlAttributes.PERSISTENT_NAMEID_SYNTHENTIC_ATTRIBUTE;
import static org.elasticsearch.xpack.security.authc.saml.SamlUtils.samlException;
import static org.opensaml.saml.saml2.core.SubjectConfirmation.METHOD_BEARER;

/**
 * Processes the IdP's SAML Response for our AuthnRequest, validates it, and extracts the relevant properties.
 */
class SamlAuthenticator extends SamlResponseHandler {

    private static final String RESPONSE_TAG_NAME = "Response";
    private static final Set<String> SPECIAL_ATTRIBUTE_NAMES = Set.of(NAMEID_SYNTHENTIC_ATTRIBUTE, PERSISTENT_NAMEID_SYNTHENTIC_ATTRIBUTE);

    SamlAuthenticator(Clock clock, IdpConfiguration idp, SpConfiguration sp, TimeValue maxSkew) {
        super(clock, idp, sp, maxSkew);
    }

    /**
     * Processes the provided SAML response within the provided token and, if valid, extracts the relevant attributes from it.
     *
     * @throws org.elasticsearch.ElasticsearchSecurityException If the SAML is invalid for this realm/configuration
     */
    SamlAttributes authenticate(SamlToken token) {
        final Element root = parseSamlMessage(token.getContent());
        if (RESPONSE_TAG_NAME.equals(root.getLocalName()) && SAML_NAMESPACE.equals(root.getNamespaceURI())) {
            try {
                return authenticateResponse(root, token.getAllowedSamlRequestIds());
            } catch (ElasticsearchSecurityException e) {
                logger.trace(
                    "Rejecting SAML response [{}...] because {}",
                    Strings.cleanTruncate(SamlUtils.toString(root), 512),
                    e.getMessage()
                );
                throw e;
            }
        } else {
            throw samlException(
                "SAML content [{}] should have a root element of Namespace=[{}] Tag=[{}]",
                root,
                SAML_NAMESPACE,
                RESPONSE_TAG_NAME
            );
        }
    }

    private SamlAttributes authenticateResponse(Element element, Collection<String> allowedSamlRequestIds) {
        final Response response = buildXmlObject(element, Response.class);
        if (response == null) {
            throw samlException("Cannot convert element {} into Response object", element);
        }
        if (logger.isTraceEnabled()) {
            logger.trace(SamlUtils.describeSamlObject(response));
        }
        final boolean requireSignedAssertions;
        if (response.isSigned()) {
            validateSignature(response.getSignature());
            requireSignedAssertions = false;
        } else {
            requireSignedAssertions = true;
        }

        checkInResponseTo(response, allowedSamlRequestIds);
        checkStatus(response.getStatus());
        checkIssuer(response.getIssuer(), response);
        checkResponseDestination(response);

        Tuple<Assertion, List<Attribute>> details = extractDetails(response, allowedSamlRequestIds, requireSignedAssertions);
        final Assertion assertion = details.v1();
        final SamlNameId nameId = SamlNameId.forSubject(assertion.getSubject());
        final String session = getSessionIndex(assertion);
        final List<SamlAttributes.SamlAttribute> attributes = details.v2().stream().map(SamlAttributes.SamlAttribute::new).toList();
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("The SAML Assertion contained the following attributes: \n");
            for (SamlAttributes.SamlAttribute attr : attributes) {
                sb.append(attr).append("\n");
            }
            logger.trace(sb.toString());
        }
        if (attributes.isEmpty() && nameId == null) {
            logger.debug(
                "The Attribute Statements of SAML Response with ID [{}] contained no attributes and the SAML Assertion Subject "
                    + "did not contain a SAML NameID. Please verify that the Identity Provider configuration with regards to attribute "
                    + "release is correct. ",
                response.getID()
            );
            throw samlException("Could not process any SAML attributes in {}", response.getElementQName());
        }

        return new SamlAttributes(nameId, session, attributes);
    }

    private String getSessionIndex(Assertion assertion) {
        return assertion.getAuthnStatements().stream().map(as -> as.getSessionIndex()).filter(Objects::nonNull).findFirst().orElse(null);
    }

    private void checkResponseDestination(Response response) {
        final String asc = getSpConfiguration().getAscUrl();
        if (asc.equals(response.getDestination()) == false) {
            if (response.isSigned() || Strings.hasText(response.getDestination())) {
                throw samlException(
                    "SAML response " + response.getID() + " is for destination " + response.getDestination() + " but this realm uses " + asc
                );
            }
        }
    }

    private Tuple<Assertion, List<Attribute>> extractDetails(
        Response response,
        Collection<String> allowedSamlRequestIds,
        boolean requireSignedAssertions
    ) {
        final int assertionCount = response.getAssertions().size() + response.getEncryptedAssertions().size();
        if (assertionCount > 1) {
            throw samlException("Expecting only 1 assertion, but response contains multiple (" + assertionCount + ")");
        }
        for (Assertion assertion : response.getAssertions()) {
            return new Tuple<>(assertion, processAssertion(assertion, requireSignedAssertions, allowedSamlRequestIds));
        }
        for (EncryptedAssertion encrypted : response.getEncryptedAssertions()) {
            Assertion assertion = decrypt(encrypted);
            moveToNewDocument(assertion);
            assertion.getDOM().setIdAttribute("ID", true);
            return new Tuple<>(assertion, processAssertion(assertion, requireSignedAssertions, allowedSamlRequestIds));
        }
        throw samlException("No assertions found in SAML response");
    }

    private void moveToNewDocument(XMLObject xmlObject) {
        final Element element = xmlObject.getDOM();
        final Document doc = element.getOwnerDocument().getImplementation().createDocument(null, null, null);
        doc.adoptNode(element);
        doc.appendChild(element);
    }

    private Assertion decrypt(EncryptedAssertion encrypted) {
        if (decrypter == null) {
            throw samlException("SAML assertion [" + text(encrypted, 32) + "] is encrypted, but no decryption key is available");
        }
        try {
            return decrypter.decrypt(encrypted);
        } catch (DecryptionException e) {
            logger.debug(
                () -> format(
                    "Failed to decrypt SAML assertion [%s] with [%s]",
                    text(encrypted, 512),
                    describe(getSpConfiguration().getEncryptionCredentials())
                ),
                e
            );
            throw samlException("Failed to decrypt SAML assertion " + text(encrypted, 32), e);
        }
    }

    private List<Attribute> processAssertion(Assertion assertion, boolean requireSignature, Collection<String> allowedSamlRequestIds) {
        if (logger.isTraceEnabled()) {
            logger.trace("(Possibly decrypted) Assertion: {}", SamlUtils.getXmlContent(assertion, true));
            logger.trace(SamlUtils.describeSamlObject(assertion));
        }
        // Do not further process unsigned Assertions
        if (assertion.isSigned()) {
            validateSignature(assertion.getSignature());
        } else if (requireSignature) {
            throw samlException("Assertion [{}] is not signed, but a signature is required", assertion.getElementQName());
        }

        checkConditions(assertion.getConditions());
        checkIssuer(assertion.getIssuer(), assertion);
        checkSubject(assertion.getSubject(), assertion, allowedSamlRequestIds);
        checkAuthnStatement(assertion.getAuthnStatements());

        List<Attribute> attributes = new ArrayList<>();
        for (AttributeStatement statement : assertion.getAttributeStatements()) {
            logger.trace(
                "SAML AttributeStatement has [{}] attributes and [{}] encrypted attributes",
                statement.getAttributes().size(),
                statement.getEncryptedAttributes().size()
            );
            attributes.addAll(statement.getAttributes());
            for (EncryptedAttribute enc : statement.getEncryptedAttributes()) {
                final Attribute attribute = decrypt(enc);
                if (attribute != null) {
                    logger.trace("Successfully decrypted attribute: {}" + SamlUtils.getXmlContent(attribute, true));
                    attributes.add(attribute);
                }
            }
        }

        warnOnSpecialAttributeNames(assertion, attributes);

        return attributes;
    }

    private void checkAuthnStatement(List<AuthnStatement> authnStatements) {
        if (authnStatements.size() != 1) {
            throw samlException(
                "SAML Assertion subject contains [{}] Authn Statements while exactly one was expected.",
                authnStatements.size()
            );
        }
        final AuthnStatement authnStatement = authnStatements.get(0);
        // "past now" that is now - the maximum skew we will tolerate. Essentially "if our clock is 2min fast, what time is it now?"
        final Instant now = now();
        final Instant pastNow = now.minusMillis(maxSkewInMillis());
        if (authnStatement.getSessionNotOnOrAfter() != null && pastNow.isBefore(authnStatement.getSessionNotOnOrAfter()) == false) {
            throw samlException(
                "Rejecting SAML assertion's Authentication Statement because [{}] is on/after [{}]",
                pastNow,
                authnStatement.getSessionNotOnOrAfter()
            );
        }
        List<String> reqAuthnCtxClassRef = this.getSpConfiguration().getReqAuthnCtxClassRef();
        if (reqAuthnCtxClassRef.isEmpty() == false) {
            String authnCtxClassRefValue = null;
            if (authnStatement.getAuthnContext() != null && authnStatement.getAuthnContext().getAuthnContextClassRef() != null) {
                authnCtxClassRefValue = authnStatement.getAuthnContext().getAuthnContextClassRef().getURI();
            }
            if (Strings.isNullOrEmpty(authnCtxClassRefValue) || reqAuthnCtxClassRef.contains(authnCtxClassRefValue) == false) {
                throw samlException(
                    "Rejecting SAML assertion as the AuthnContextClassRef [{}] is not one of the ({}) that were "
                        + "requested in the corresponding AuthnRequest",
                    authnCtxClassRefValue,
                    reqAuthnCtxClassRef
                );
            }
        }
    }

    private Attribute decrypt(EncryptedAttribute encrypted) {
        if (decrypter == null) {
            logger.info("SAML message has encrypted attribute [" + text(encrypted, 32) + "], but no encryption key has been configured");
            return null;
        }
        try {
            return decrypter.decrypt(encrypted);
        } catch (DecryptionException e) {
            logger.info("Failed to decrypt SAML attribute " + text(encrypted, 32), e);
            return null;
        }
    }

    private void checkConditions(Conditions conditions) {
        if (conditions != null) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "SAML Assertion was intended for the following Service providers: {}",
                    conditions.getAudienceRestrictions().stream().map(r -> text(r, 32)).collect(Collectors.joining(" | "))
                );
                logger.trace("SAML Assertion is only valid between: " + conditions.getNotBefore() + " and " + conditions.getNotOnOrAfter());
            }
            checkAudienceRestrictions(conditions.getAudienceRestrictions());
            checkLifetimeRestrictions(conditions);
        }
    }

    private void checkSubject(Subject assertionSubject, XMLObject parent, Collection<String> allowedSamlRequestIds) {

        if (assertionSubject == null) {
            throw samlException("SAML Assertion ({}) has no Subject", text(parent, 16));
        }
        final List<SubjectConfirmationData> confirmationData = assertionSubject.getSubjectConfirmations()
            .stream()
            .filter(data -> data.getMethod().equals(METHOD_BEARER))
            .map(SubjectConfirmation::getSubjectConfirmationData)
            .filter(Objects::nonNull)
            .toList();
        if (confirmationData.size() != 1) {
            throw samlException(
                "SAML Assertion subject contains [{}] bearer SubjectConfirmation, while exactly one was expected.",
                confirmationData.size()
            );
        }
        if (logger.isTraceEnabled()) {
            logger.trace("SAML Assertion Subject Confirmation intended recipient is: " + confirmationData.get(0).getRecipient());
            logger.trace("SAML Assertion Subject Confirmation is only valid before: " + confirmationData.get(0).getNotOnOrAfter());
            logger.trace("SAML Assertion Subject Confirmation is in response to: " + confirmationData.get(0).getInResponseTo());
        }
        checkRecipient(confirmationData.get(0));
        checkLifetimeRestrictions(confirmationData.get(0));
        checkSubjectInResponseTo(confirmationData.get(0), allowedSamlRequestIds);
    }

    private void checkSubjectInResponseTo(SubjectConfirmationData subjectConfirmationData, Collection<String> allowedSamlRequestIds) {
        // Allow for IdP initiated SSO where InResponseTo MUST be missing
        if (Strings.hasText(subjectConfirmationData.getInResponseTo())
            && allowedSamlRequestIds.contains(subjectConfirmationData.getInResponseTo()) == false) {
            throw samlException(
                "SAML Assertion SubjectConfirmationData is in-response-to [{}] but expected one of [{}]",
                subjectConfirmationData.getInResponseTo(),
                allowedSamlRequestIds
            );
        }
    }

    private void checkRecipient(SubjectConfirmationData subjectConfirmationData) {
        final SpConfiguration sp = getSpConfiguration();
        if (sp.getAscUrl().equals(subjectConfirmationData.getRecipient()) == false) {
            throw samlException(
                "SAML Assertion SubjectConfirmationData Recipient [{}] does not match expected value [{}]",
                subjectConfirmationData.getRecipient(),
                sp.getAscUrl()
            );
        }
    }

    private void checkAudienceRestrictions(List<AudienceRestriction> restrictions) {
        if (restrictions.stream().allMatch(this::checkAudienceRestriction) == false) {
            throw samlException(
                "Conditions [{}] do not match required audience [{}]",
                restrictions.stream().map(r -> text(r, 56, 8)).collect(Collectors.joining(" | ")),
                getSpConfiguration().getEntityId()
            );
        }
    }

    private boolean checkAudienceRestriction(AudienceRestriction restriction) {
        final String spEntityId = this.getSpConfiguration().getEntityId();
        if (restriction.getAudiences().stream().map(Audience::getURI).anyMatch(spEntityId::equals) == false) {
            restriction.getAudiences().stream().map(Audience::getURI).forEach(uri -> {
                int diffChar;
                for (diffChar = 0; diffChar < uri.length() && diffChar < spEntityId.length(); diffChar++) {
                    if (uri.charAt(diffChar) != spEntityId.charAt(diffChar)) {
                        break;
                    }
                }
                // If the difference is less than half the length of the string, show it in detail
                if (diffChar >= spEntityId.length() / 2) {
                    logger.info(
                        "Audience restriction [{}] does not match required audience [{}] "
                            + "(difference starts at character [#{}] [{}] vs [{}])",
                        uri,
                        spEntityId,
                        diffChar,
                        uri.substring(diffChar),
                        spEntityId.substring(diffChar)
                    );
                } else {
                    logger.info("Audience restriction [{}] does not match required audience [{}]", uri, spEntityId);

                }
            });
            return false;
        }
        return true;
    }

    private void checkLifetimeRestrictions(Conditions conditions) {
        // In order to compensate for clock skew we construct 2 alternate realities
        // - a "future now" that is now + the maximum skew we will tolerate. Essentially "if our clock is 2min slow, what time is it now?"
        // - a "past now" that is now - the maximum skew we will tolerate. Essentially "if our clock is 2min fast, what time is it now?"
        final Instant now = now();
        final Instant futureNow = now.plusMillis(maxSkewInMillis());
        final Instant pastNow = now.minusMillis(maxSkewInMillis());
        if (conditions.getNotBefore() != null && futureNow.isBefore(conditions.getNotBefore())) {
            throw samlException("Rejecting SAML assertion because [{}] is before [{}]", futureNow, conditions.getNotBefore());
        }
        if (conditions.getNotOnOrAfter() != null && pastNow.isBefore(conditions.getNotOnOrAfter()) == false) {
            throw samlException("Rejecting SAML assertion because [{}] is on/after [{}]", pastNow, conditions.getNotOnOrAfter());
        }
    }

    private void checkLifetimeRestrictions(SubjectConfirmationData subjectConfirmationData) {
        validateNotOnOrAfter(subjectConfirmationData.getNotOnOrAfter());
    }

    private void warnOnSpecialAttributeNames(Assertion assertion, List<Attribute> attributes) {
        attributes.forEach(attribute -> {
            warnOnSpecialAttributeName(assertion, attribute.getName(), "name");

            String attributeFriendlyName = attribute.getFriendlyName();
            if (attributeFriendlyName != null) {
                warnOnSpecialAttributeName(assertion, attributeFriendlyName, "friendly name");
            }
        });
    }

    private void warnOnSpecialAttributeName(Assertion assertion, String attributeName, String fieldNameForLogMessage) {
        if (SPECIAL_ATTRIBUTE_NAMES.contains(attributeName)) {
            logger.warn(
                "SAML assertion [{}] has attribute with {} [{}] which clashes with a special attribute name. "
                    + "Attributes with a name clash may prevent authentication or interfere will role mapping. "
                    + "Change your IdP configuration to use a different attribute {}"
                    + " that will not clash with any of [{}]",
                assertion.getElementQName(),
                fieldNameForLogMessage,
                attributeName,
                fieldNameForLogMessage,
                String.join(",", SPECIAL_ATTRIBUTE_NAMES)
            );
        }
    }
}
