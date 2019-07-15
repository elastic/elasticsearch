/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchScope;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.isNullOrEmpty;
import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.IGNORE_REFERRAL_ERRORS_SETTING;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.OBJECT_CLASS_PRESENCE_FILTER;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.createFilter;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.search;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.searchForEntry;

/**
 * Resolves the groups for a user by executing a search with a filter usually that contains a group
 * object class with a attribute that matches an ID of the user
 */
class SearchGroupsResolver implements GroupsResolver {

    private final String baseDn;
    private final String filter;
    private final String userAttribute;
    private final LdapSearchScope scope;
    private final boolean ignoreReferralErrors;

    SearchGroupsResolver(RealmConfig config) {
        baseDn = config.getSetting(SearchGroupsResolverSettings.BASE_DN, () -> {
            throw new IllegalArgumentException("base_dn must be specified");
        });
        filter = config.getSetting(SearchGroupsResolverSettings.FILTER);
        userAttribute = config.getSetting(SearchGroupsResolverSettings.USER_ATTRIBUTE);
        scope = config.getSetting(SearchGroupsResolverSettings.SCOPE);
        ignoreReferralErrors = config.getSetting(IGNORE_REFERRAL_ERRORS_SETTING);
    }

    @Override
    public void resolve(LDAPInterface connection, String userDn, TimeValue timeout, Logger logger,
                        Collection<Attribute> attributes, ActionListener<List<String>> listener) {
        getUserId(userDn, attributes, connection, timeout, ActionListener.wrap((userId) -> {
            if (userId == null) {
                listener.onResponse(List.of());
            } else {
                try {
                    Filter userFilter = createFilter(filter, userId);
                    search(connection, baseDn, scope.scope(), userFilter,
                            Math.toIntExact(timeout.seconds()), ignoreReferralErrors,
                            ActionListener.wrap(
                                    (results) -> listener.onResponse(results
                                            .stream()
                                            .map((r) -> r.getDN())
                                            .collect(Collectors.toUnmodifiableList())
                                    ),
                                    listener::onFailure),
                            SearchRequest.NO_ATTRIBUTES);
                } catch (LDAPException e) {
                    listener.onFailure(e);
                }
            }
        }, listener::onFailure));
    }

    @Override
    public String[] attributes() {
        if (Strings.hasLength(userAttribute)) {
            return new String[] { userAttribute };
        }
        return null;
    }

    private void getUserId(String dn, Collection<Attribute> attributes, LDAPInterface connection,
                           TimeValue timeout, ActionListener<String> listener) {
        if (isNullOrEmpty(userAttribute) || userAttribute.equals("dn")) {
            listener.onResponse(dn);
        } else if (attributes != null) {
            final String value = attributes.stream()
                    .filter((attribute) -> attribute.getName().equals(userAttribute))
                    .map(Attribute::getValue)
                    .findFirst()
                    .orElse(null);
            listener.onResponse(value);
        } else {
            readUserAttribute(connection, dn, timeout, listener);
        }
    }

    void readUserAttribute(LDAPInterface connection, String userDn, TimeValue timeout,
                           ActionListener<String> listener) {
        searchForEntry(connection, userDn, SearchScope.BASE, OBJECT_CLASS_PRESENCE_FILTER,
                Math.toIntExact(timeout.seconds()), ignoreReferralErrors,
                ActionListener.wrap((entry) -> {
                    if (entry == null || entry.hasAttribute(userAttribute) == false) {
                        listener.onResponse(null);
                    } else {
                        listener.onResponse(entry.getAttributeValue(userAttribute));
                    }
                }, listener::onFailure),
                userAttribute);
    }


}
