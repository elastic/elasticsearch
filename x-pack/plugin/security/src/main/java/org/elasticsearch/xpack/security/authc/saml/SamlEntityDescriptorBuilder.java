/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;

import java.util.Locale;
import java.util.Map;

public class SamlEntityDescriptorBuilder {
    private final EntityDescriptor entityDescriptor;

    public EntityDescriptor getEntityDescriptor() {
        return entityDescriptor;
    }

    public SamlEntityDescriptorBuilder(SamlRealm samlRealm) throws Exception{
        final SpConfiguration spConfig = samlRealm.getLogoutHandler().getSpConfiguration();
        final SamlSpMetadataBuilder builder = new SamlSpMetadataBuilder(samlRealm)
            .encryptionCredentials(spConfig.getEncryptionCredentials())
            .signingCredential(spConfig.getSigningConfiguration().getCredential())
            .authnRequestsSigned(spConfig.getSigningConfiguration().shouldSign(AuthnRequest.DEFAULT_ELEMENT_LOCAL_NAME));
        entityDescriptor = builder.build();
    }

    public SamlEntityDescriptorBuilder(RealmConfig realm, boolean batch, String serviceName, String orgName, String orgUrl,
                                       String orgDisplayName, boolean contacts, Locale locale, Map<String, String> attributes,
                                       Terminal terminal) throws Exception {
        final Settings realmSettings = realm.settings().getByPrefix(RealmSettings.realmSettingPrefix(realm.identifier()));
        terminal.println(Terminal.Verbosity.VERBOSE,
            "Using realm configuration\n=====\n" + realmSettings.toDelimitedString('\n') + "=====");
        terminal.println(Terminal.Verbosity.VERBOSE, "Using locale: " + locale.toLanguageTag());

        final SpConfiguration spConfig = SamlRealm.getSpConfiguration(realm);
        final SamlSpMetadataBuilder builder = new SamlSpMetadataBuilder(locale, spConfig.getEntityId())
            .assertionConsumerServiceUrl(spConfig.getAscUrl())
            .singleLogoutServiceUrl(spConfig.getLogoutUrl())
            .encryptionCredentials(spConfig.getEncryptionCredentials())
            .signingCredential(spConfig.getSigningConfiguration().getCredential())
            .authnRequestsSigned(spConfig.getSigningConfiguration().shouldSign(AuthnRequest.DEFAULT_ELEMENT_LOCAL_NAME))
            .nameIdFormat(realm.getSetting(SamlRealmSettings.NAMEID_FORMAT))
            .serviceName(serviceName);

        for (String attr : attributes.keySet()) {
            final String name;
            String friendlyName;
            final String settingName = attributes.get(attr);
            final String attributeSource = settingName == null ? "command line" : '"' + settingName + '"';
            if (attr.contains(":")) {
                name = attr;
                if (batch) {
                    friendlyName = settingName;
                } else {
                    friendlyName = terminal.readText("What is the friendly name for " +
                        attributeSource
                        + " attribute \"" + attr + "\" [default: " +
                        (settingName == null ? "none" : settingName) +
                        "] ");
                    if (Strings.isNullOrEmpty(friendlyName)) {
                        friendlyName = settingName;
                    }
                }
            } else {
                if (batch) {
                    throw new UserException(ExitCodes.CONFIG, "Option batch is specified, but attribute "
                        + attr + " appears to be a FriendlyName value");
                }
                friendlyName = attr;
                name = requireText(terminal,
                    "What is the standard (urn) name for " + attributeSource + " attribute \"" + attr + "\" (required): ");
            }
            terminal.println(Terminal.Verbosity.VERBOSE, "Requesting attribute '" + name + "' (FriendlyName: '" + friendlyName + "')");
            builder.withAttribute(friendlyName, name);
        }

        if (orgName != null && orgUrl != null) {
            builder.organization(orgName, orgDisplayName, orgUrl);
        }

        if (contacts) {
            terminal.println("\nPlease enter the personal details for each contact to be included in the metadata");
            do {
                final String givenName = requireText(terminal, "What is the given name for the contact: ");
                final String surName = requireText(terminal, "What is the surname for the contact: ");
                final String displayName = givenName + ' ' + surName;
                final String email = requireText(terminal, "What is the email address for " + displayName + ": ");
                String type;
                while (true) {
                    type = requireText(terminal, "What is the contact type for " + displayName + ": ");
                    if (SamlSpMetadataBuilder.ContactInfo.TYPES.containsKey(type)) {
                        break;
                    } else {
                        terminal.errorPrintln("Type '" + type + "' is not valid. Valid values are "
                            + Strings.collectionToCommaDelimitedString(SamlSpMetadataBuilder.ContactInfo.TYPES.keySet()));
                    }
                }
                builder.withContact(type, givenName, surName, email);
            } while (terminal.promptYesNo("Enter details for another contact", true));
        }
        entityDescriptor = builder.build();
    }

    private String requireText(Terminal terminal, String prompt) {
        String value = null;
        while (Strings.isNullOrEmpty(value)) {
            value = terminal.readText(prompt);
        }
        return value;
    }
}
