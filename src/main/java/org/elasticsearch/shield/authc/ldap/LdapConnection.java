/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapConnection;
import org.elasticsearch.shield.authc.support.ldap.ClosableNamingEnumeration;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Encapsulates jndi/ldap functionality into one authenticated connection.  The constructor is package scoped, assuming
 * instances of this connection will be produced by the LdapConnectionFactory.open() methods.
 * <p/>
 * A standard looking usage pattern could look like this:
 * <pre>
 * try (LdapConnection session = ldapFac.bindXXX(...);
 * ...do stuff with the session
 * }
 * </pre>
 */
public class LdapConnection extends AbstractLdapConnection {

    private final int timeoutMilliseconds;
    private final boolean isGroupSubTreeSearch;
    private final boolean isFindGroupsByAttribute;
    private final String groupSearchDN;
    private final String groupAttribute = "memberOf";
    private final String userAttributeForGroupMembership;
    private final String groupSearchFilter;

    /**
     * This object is intended to be constructed by the LdapConnectionFactory
     */
    LdapConnection(ESLogger logger, DirContext ctx, String bindDN, int timeoutMilliseconds, boolean isFindGroupsByAttribute, boolean isGroupSubTreeSearch,
                   @Nullable String groupSearchFilter, @Nullable String groupSearchDN, @Nullable String userAttributeForGroupMembership) {
        super(logger, ctx, bindDN);
        this.isGroupSubTreeSearch = isGroupSubTreeSearch;
        this.groupSearchFilter = groupSearchFilter;
        this.groupSearchDN = groupSearchDN;
        this.isFindGroupsByAttribute = isFindGroupsByAttribute;
        this.userAttributeForGroupMembership = userAttributeForGroupMembership;
        this.timeoutMilliseconds = timeoutMilliseconds;
    }

    /**
     * Fetches the groups that the user is a member of
     *
     * @return List of group membership
     */
    @Override
    public List<String> groups() {
        List<String> groups = isFindGroupsByAttribute ? getGroupsFromUserAttrs() : getGroupsFromSearch();
        if (logger.isDebugEnabled()) {
            logger.debug("found groups [{}] for userDN [{}]", groups, this.bindDn);
        }
        return groups;
    }

    /**
     * Fetches the groups of a user by doing a search.  This could be abstracted out into a strategy class or through
     * an inherited class (with groups as the template method).
     * @return fully distinguished names of the roles
     */
    public List<String> getGroupsFromSearch() {
        String userIdentifier = userAttributeForGroupMembership == null ? bindDn : readUserAttribute(userAttributeForGroupMembership);
        if (logger.isTraceEnabled()) {
            logger.trace("user identifier for group lookup is [{}]", userIdentifier);
        }
        List<String> groups = new LinkedList<>();
        SearchControls search = new SearchControls();
        search.setReturningAttributes(Strings.EMPTY_ARRAY);
        search.setSearchScope(this.isGroupSubTreeSearch ? SearchControls.SUBTREE_SCOPE : SearchControls.ONELEVEL_SCOPE);
        search.setTimeLimit(timeoutMilliseconds);

        try (ClosableNamingEnumeration<SearchResult> results = new ClosableNamingEnumeration<>(
                jndiContext.search(groupSearchDN, groupSearchFilter, new Object[] {userIdentifier}, search))) {
            while (results.hasMoreElements()) {
                groups.add(results.next().getNameInNamespace());
            }
        } catch (NamingException | LdapException e ) {
            throw new LdapException("could not search for an LDAP group", bindDn, e);
        }
        return groups;
    }

    /**
     * Fetches the groups from the user attributes (if supported).  This method could later be abstracted out
     * into a strategy class
     *
     * @return list of groups the user is a member of.
     */
    public List<String> getGroupsFromUserAttrs() {
        List<String> groupDns = new LinkedList<>();
        try {
            Attributes results = jndiContext.getAttributes(bindDn, new String[] { groupAttribute });
            try (ClosableNamingEnumeration<? extends Attribute> ae = new ClosableNamingEnumeration<>(results.getAll())) {
                while (ae.hasMore()) {
                    Attribute attr = ae.next();
                    for (NamingEnumeration attrEnum = attr.getAll(); attrEnum.hasMore(); ) {
                        Object val = attrEnum.next();
                        if (val instanceof String) {
                            String stringVal = (String) val;
                            groupDns.add(stringVal);
                        }
                    }
                }
            }
        } catch (NamingException | LdapException e) {
            throw new LdapException("could not look up group attributes for user", bindDn, e);
        }
        return groupDns;
    }

    String readUserAttribute(String userAttribute) {
        try {
            Attributes results = jndiContext.getAttributes(bindDn, new String[]{userAttribute});
            Attribute attribute = results.get(userAttribute);
            if (results.size() == 0) {
                throw new LdapException("No results returned for attribute [" + userAttribute + "]", bindDn);
            }
            return (String) attribute.get();
        } catch (NamingException  e) {
            throw new LdapException("Could not look attribute [" + userAttribute + "]", bindDn, e);
        } catch (ClassCastException e) {
            throw new LdapException("Returned ldap attribute [" + userAttribute + "] is not of type String", bindDn, e);
        }
    }
}
