/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.unboundid.ldap.sdk.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;

import java.util.LinkedList;
import java.util.List;

import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.*;

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
            throw new ShieldSettingsException("base_dn must be specified");
        }
        filter = settings.get("filter", GROUP_SEARCH_DEFAULT_FILTER);
        userAttribute = settings.get("user_attribute");
        scope = LdapSearchScope.resolve(settings.get("scope"), LdapSearchScope.SUB_TREE);
    }

    @Override
    public List<String> resolve(LDAPConnection connection, String userDn, TimeValue timeout, ESLogger logger) {
        List<String> groups = new LinkedList<>();

        String userId = userAttribute != null ? readUserAttribute(connection, userDn, timeout, logger) : userDn;
        try {
            SearchRequest searchRequest = new SearchRequest(baseDn, scope.scope(), createFilter(filter, userId), Strings.EMPTY_ARRAY);
            searchRequest.setTimeLimitSeconds(Ints.checkedCast(timeout.seconds()));
            SearchResult results = search(connection, searchRequest, logger);
            for (SearchResultEntry entry : results.getSearchEntries()) {
                groups.add(entry.getDN());
            }
        } catch (LDAPException e) {
            throw new ShieldLdapException("could not search for LDAP groups", userDn, e);
        }

        return groups;
    }

    String readUserAttribute(LDAPConnection connection, String userDn, TimeValue timeout, ESLogger logger) {
        try {
            SearchRequest request = new SearchRequest(userDn, SearchScope.BASE, OBJECT_CLASS_PRESENCE_FILTER, userAttribute);
            request.setTimeLimitSeconds(Ints.checkedCast(timeout.seconds()));
            SearchResultEntry results = searchForEntry(connection, request, logger);
            Attribute attribute = results.getAttribute(userAttribute);
            if (attribute == null) {
                throw new ShieldLdapException("no results returned for attribute [" + userAttribute + "]", userDn);
            }
            return attribute.getValue();
        } catch (LDAPException e) {
            throw new ShieldLdapException("could not retrieve attribute [" + userAttribute + "]", userDn, e);
        }
    }
}
