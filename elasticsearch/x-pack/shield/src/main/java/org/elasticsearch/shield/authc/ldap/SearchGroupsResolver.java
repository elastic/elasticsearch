/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.shield.support.Exceptions;

import java.util.LinkedList;
import java.util.List;

import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.OBJECT_CLASS_PRESENCE_FILTER;
import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.createFilter;
import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.search;
import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.searchForEntry;

/**
*
*/
class SearchGroupsResolver implements GroupsResolver {

    private static final String GROUP_SEARCH_DEFAULT_FILTER = "(&" +
            "(|(objectclass=groupOfNames)(objectclass=groupOfUniqueNames)(objectclass=group))" +
            "(|(uniqueMember={0})(member={0})))";

    private final String baseDn;
    private final String filter;
    private final String userAttribute;
    private final LdapSearchScope scope;

    public SearchGroupsResolver(Settings settings) {
        baseDn = settings.get("base_dn");
        if (baseDn == null) {
            throw new IllegalArgumentException("base_dn must be specified");
        }
        filter = settings.get("filter", GROUP_SEARCH_DEFAULT_FILTER);
        userAttribute = settings.get("user_attribute");
        scope = LdapSearchScope.resolve(settings.get("scope"), LdapSearchScope.SUB_TREE);
    }

    @Override
    public List<String> resolve(LDAPInterface connection, String userDn, TimeValue timeout, ESLogger logger) {
        List<String> groups = new LinkedList<>();

        String userId = userAttribute != null ? readUserAttribute(connection, userDn, timeout, logger) : userDn;
        try {
            SearchRequest searchRequest = new SearchRequest(baseDn, scope.scope(), createFilter(filter, userId), 
                    SearchRequest.NO_ATTRIBUTES);
            searchRequest.setTimeLimitSeconds(Math.toIntExact(timeout.seconds()));
            SearchResult results = search(connection, searchRequest, logger);
            for (SearchResultEntry entry : results.getSearchEntries()) {
                groups.add(entry.getDN());
            }
        } catch (LDAPException e) {
            throw Exceptions.authenticationError("could not search for LDAP groups for DN [{}]", e, userDn);
        }

        return groups;
    }

    String readUserAttribute(LDAPInterface connection, String userDn, TimeValue timeout, ESLogger logger) {
        try {
            SearchRequest request = new SearchRequest(userDn, SearchScope.BASE, OBJECT_CLASS_PRESENCE_FILTER, userAttribute);
            request.setTimeLimitSeconds(Math.toIntExact(timeout.seconds()));
            SearchResultEntry results = searchForEntry(connection, request, logger);
            Attribute attribute = results.getAttribute(userAttribute);
            if (attribute == null) {
                throw Exceptions.authenticationError("no results returned for DN [{}] attribute [{}]", userDn, userAttribute);
            }
            return attribute.getValue();
        } catch (LDAPException e) {
            throw Exceptions.authenticationError("could not retrieve attribute [{}] for DN [{}]", e, userAttribute, userDn);
        }
    }
}
