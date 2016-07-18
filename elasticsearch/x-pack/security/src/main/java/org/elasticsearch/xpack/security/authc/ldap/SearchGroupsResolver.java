/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    public List<String> resolve(LDAPInterface connection, String userDn, TimeValue timeout, ESLogger logger,
                                Collection<Attribute> attributes) throws LDAPException {
        String userId = getUserId(userDn, attributes, connection, timeout, logger);
        if (userId == null) {
            // attributes were queried but the requested wasn't found
            return Collections.emptyList();
        }

        SearchRequest searchRequest = new SearchRequest(baseDn, scope.scope(), createFilter(filter, userId),
                SearchRequest.NO_ATTRIBUTES);
        searchRequest.setTimeLimitSeconds(Math.toIntExact(timeout.seconds()));
        SearchResult results = search(connection, searchRequest, logger);
        List<String> groups = new ArrayList<>(results.getSearchEntries().size());
        for (SearchResultEntry entry : results.getSearchEntries()) {
            groups.add(entry.getDN());
        }
        return groups;
    }

    public String[] attributes() {
        if (userAttribute != null) {
            return new String[] { userAttribute };
        }
        return null;
    }

    private String getUserId(String dn, Collection<Attribute> attributes, LDAPInterface connection, TimeValue
            timeout, ESLogger logger) throws LDAPException {
        if (userAttribute == null) {
            return dn;
        }

        if (attributes != null) {
            for (Attribute attribute : attributes) {
                if (attribute.getName().equals(userAttribute)) {
                    return attribute.getValue();
                }
            }
        }

        return readUserAttribute(connection, dn, timeout, logger);
    }

    String readUserAttribute(LDAPInterface connection, String userDn, TimeValue timeout, ESLogger logger) throws LDAPException {
        SearchRequest request = new SearchRequest(userDn, SearchScope.BASE, OBJECT_CLASS_PRESENCE_FILTER, userAttribute);
        request.setTimeLimitSeconds(Math.toIntExact(timeout.seconds()));
        SearchResultEntry results = searchForEntry(connection, request, logger);
        if (results == null) {
            return null;
        }
        Attribute attribute = results.getAttribute(userAttribute);
        if (attribute == null) {
            return null;
        }
        return attribute.getValue();
    }
}
