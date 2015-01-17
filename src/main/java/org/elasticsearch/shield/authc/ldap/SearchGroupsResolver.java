/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapConnection;
import org.elasticsearch.shield.authc.support.ldap.ClosableNamingEnumeration;
import org.elasticsearch.shield.authc.support.ldap.SearchScope;

import javax.naming.NamingException;
import javax.naming.directory.*;
import java.util.LinkedList;
import java.util.List;

/**
*
*/
class SearchGroupsResolver implements AbstractLdapConnection.GroupsResolver {

    private static final String GROUP_SEARCH_DEFAULT_FILTER = "(&" +
            "(|(objectclass=groupOfNames)(objectclass=groupOfUniqueNames)(objectclass=group))" +
            "(|(uniqueMember={0})(member={0})))";

    private final String baseDn;
    private final String filter;
    private final String userAttribute;
    private final SearchScope scope;

    public SearchGroupsResolver(Settings settings) {
        baseDn = settings.get("base_dn");
        filter = settings.get("filter", GROUP_SEARCH_DEFAULT_FILTER);
        userAttribute = filter == null ? null : settings.get("user_attribute");
        scope = SearchScope.resolve(settings.get("scope"), SearchScope.SUB_TREE);
    }

    public SearchGroupsResolver(String baseDn, String filter, String userAttribute, SearchScope scope) {
        this.baseDn = baseDn;
        this.filter = filter;
        this.userAttribute = userAttribute;
        this.scope = scope;
    }

    @Override
    public List<String> resolve(DirContext ctx, String userDn, TimeValue timeout) {
        List<String> groups = new LinkedList<>();

        String userId = userAttribute != null ? readUserAttribute(ctx, userDn, userDn) : userDn;
        SearchControls search = new SearchControls();
        search.setReturningAttributes(Strings.EMPTY_ARRAY);
        search.setSearchScope(scope.scope());
        search.setTimeLimit((int) timeout.millis());

        try (ClosableNamingEnumeration<SearchResult> results = new ClosableNamingEnumeration<>(
                ctx.search(baseDn, filter, new Object[] { userId }, search))) {
            while (results.hasMoreElements()) {
                groups.add(results.next().getNameInNamespace());
            }
        } catch (NamingException | LdapException e ) {
            throw new LdapException("could not search for an LDAP group", userDn, e);
        }
        return groups;
    }

    String readUserAttribute(DirContext ctx, String userDn, String userAttribute) {
        try {
            Attributes results = ctx.getAttributes(userDn, new String[]{userAttribute});
            Attribute attribute = results.get(userAttribute);
            if (results.size() == 0) {
                throw new LdapException("No results returned for attribute [" + userAttribute + "]", userDn);
            }
            return (String) attribute.get();
        } catch (NamingException  e) {
            throw new LdapException("Could not look attribute [" + userAttribute + "]", userDn, e);
        } catch (ClassCastException e) {
            throw new LdapException("Returned ldap attribute [" + userAttribute + "] is not of type String", userDn, e);
        }
    }
}
