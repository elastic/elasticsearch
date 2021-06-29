/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchScope;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.UserAttributeGroupsResolverSettings;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.IGNORE_REFERRAL_ERRORS_SETTING;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.OBJECT_CLASS_PRESENCE_FILTER;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.searchForEntry;

/**
* Resolves the groups of a user based on the value of a attribute of the user's ldap entry
*/
class UserAttributeGroupsResolver implements GroupsResolver {

    private final String attribute;
    private final boolean ignoreReferralErrors;

    UserAttributeGroupsResolver(RealmConfig realmConfig) {
        this(realmConfig.getSetting(UserAttributeGroupsResolverSettings.ATTRIBUTE), realmConfig.getSetting(IGNORE_REFERRAL_ERRORS_SETTING));
    }

    private UserAttributeGroupsResolver(String attribute, boolean ignoreReferralErrors) {
        this.attribute = Objects.requireNonNull(attribute);
        this.ignoreReferralErrors = ignoreReferralErrors;
    }

    @Override
    public void resolve(LDAPInterface connection, String userDn, TimeValue timeout, Logger logger, Collection<Attribute> attributes,
                        ActionListener<List<String>> listener) {
        if (attributes != null) {
            final List<String> groups = attributes.stream()
                    .filter((attr) -> attr.getName().equals(attribute))
                    .flatMap(attr -> Arrays.stream(attr.getValues()))
                    .collect(Collectors.toUnmodifiableList());
            listener.onResponse(groups);
        } else {
            searchForEntry(connection, userDn, SearchScope.BASE, OBJECT_CLASS_PRESENCE_FILTER, Math.toIntExact(timeout.seconds()),
                    ignoreReferralErrors, ActionListener.wrap((entry) -> {
                        if (entry == null || entry.hasAttribute(attribute) == false) {
                            listener.onResponse(List.of());
                        } else {
                            listener.onResponse(List.of(entry.getAttributeValues(attribute)));
                        }
                    }, listener::onFailure), attribute);
        }
    }

    @Override
    public String[] attributes() {
        return new String[] { attribute };
    }


}
