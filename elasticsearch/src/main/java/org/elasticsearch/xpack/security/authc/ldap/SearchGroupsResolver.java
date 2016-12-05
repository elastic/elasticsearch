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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.OBJECT_CLASS_PRESENCE_FILTER;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.createFilter;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.search;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.searchForEntry;

/**
 * Resolves the groups for a user by executing a search with a filter usually that contains a group object class with a attribute that
 * matches an ID of the user
 */
class SearchGroupsResolver implements GroupsResolver {

    private static final String GROUP_SEARCH_DEFAULT_FILTER = "(&" +
            "(|(objectclass=groupOfNames)(objectclass=groupOfUniqueNames)(objectclass=group)(objectclass=posixGroup))" +
            "(|(uniqueMember={0})(member={0})(memberUid={0})))";

    private final String baseDn;
    private final String filter;
    private final String userAttribute;
    private final LdapSearchScope scope;

    SearchGroupsResolver(Settings settings) {
        baseDn = settings.get("base_dn");
        if (baseDn == null) {
            throw new IllegalArgumentException("base_dn must be specified");
        }
        filter = settings.get("filter", GROUP_SEARCH_DEFAULT_FILTER);
        userAttribute = settings.get("user_attribute");
        scope = LdapSearchScope.resolve(settings.get("scope"), LdapSearchScope.SUB_TREE);
    }

    @Override
    public void resolve(LDAPInterface connection, String userDn, TimeValue timeout, Logger logger,
                        Collection<Attribute> attributes, ActionListener<List<String>> listener) {
        getUserId(userDn, attributes, connection, timeout, ActionListener.wrap((userId) -> {
            if (userId == null) {
                listener.onResponse(Collections.emptyList());
            } else {
                try {
                    Filter userFilter = createFilter(filter, userId);
                    search(connection, baseDn, scope.scope(), userFilter, Math.toIntExact(timeout.seconds()),
                            ActionListener.wrap(
                                    (results) -> listener.onResponse(results.stream().map((r) -> r.getDN()).collect(Collectors.toList())),
                                    listener::onFailure),
                            SearchRequest.NO_ATTRIBUTES);
                } catch (LDAPException e) {
                    listener.onFailure(e);
                }
            }
        }, listener::onFailure));
    }

    public String[] attributes() {
        if (userAttribute != null) {
            return new String[] { userAttribute };
        }
        return null;
    }

    private void getUserId(String dn, Collection<Attribute> attributes, LDAPInterface connection, TimeValue timeout,
                             ActionListener<String> listener) {
        if (userAttribute == null) {
            listener.onResponse(dn);
        } else if (attributes != null) {
            final String value = attributes.stream().filter((attribute) -> attribute.getName().equals(userAttribute))
                    .map(Attribute::getValue)
                    .findFirst()
                    .orElse(null);
            listener.onResponse(value);
        } else {
            readUserAttribute(connection, dn, timeout, listener);
        }
    }

    void readUserAttribute(LDAPInterface connection, String userDn, TimeValue timeout, ActionListener<String> listener) {
        searchForEntry(connection, userDn, SearchScope.BASE, OBJECT_CLASS_PRESENCE_FILTER, Math.toIntExact(timeout.seconds()),
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
