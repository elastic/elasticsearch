/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapConnection;

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

    private static final ESLogger logger = Loggers.getLogger(LdapConnection.class);

    private final String groupSearchDN;
    private final boolean isGroupSubTreeSearch;
    private final boolean isFindGroupsByAttribute;
    private final String groupAttribute = "memberOf";
    private final int timeoutMilliseconds;

    /**
     * This object is intended to be constructed by the LdapConnectionFactory
     */
    LdapConnection(DirContext ctx, String boundName, boolean isFindGroupsByAttribute, boolean isGroupSubTreeSearch, String groupSearchDN, int timeoutMilliseconds) {
        super(ctx, boundName);
        this.isGroupSubTreeSearch = isGroupSubTreeSearch;
        this.groupSearchDN = groupSearchDN;
        this.isFindGroupsByAttribute = isFindGroupsByAttribute;
        this.timeoutMilliseconds = timeoutMilliseconds;
    }

    /**
     * Fetches the groups that the user is a member of
     *
     * @return List of group membership
     */
    @Override
    public List<String> groups() {
        List<String> groups = isFindGroupsByAttribute ? getGroupsFromUserAttrs(bindDn) : getGroupsFromSearch(bindDn);
        if (logger.isDebugEnabled()) {
            logger.debug("Found these groups [{}] for userDN [{}]", groups, this.bindDn);
        }
        return groups;
    }

    /**
     * Fetches the groups of a user by doing a search.  This could be abstracted out into a strategy class or through
     * an inherited class (with groups as the template method).
     *
     * @param userDn user fully distinguished name to fetch group membership for
     * @return fully distinguished names of the roles
     */
    public List<String> getGroupsFromSearch(String userDn) {
        List<String> groups = new LinkedList<>();
        SearchControls search = new SearchControls();
        search.setReturningAttributes(Strings.EMPTY_ARRAY);
        search.setSearchScope(this.isGroupSubTreeSearch ? SearchControls.SUBTREE_SCOPE : SearchControls.ONELEVEL_SCOPE);
        search.setTimeLimit(timeoutMilliseconds);

        //This could be made could be made configurable but it should cover all cases
        String filter = "(&" +
                "(|(objectclass=groupOfNames)(objectclass=groupOfUniqueNames)(objectclass=group)) " +
                "(|(uniqueMember={0})(member={0})))";

        try {
            NamingEnumeration<SearchResult> results = jndiContext.search(groupSearchDN, filter, new Object[] { userDn }, search);
            while (results.hasMoreElements()) {
                groups.add(results.next().getNameInNamespace());
            }
        } catch (NamingException e) {
            throw new LdapException("Could not search for an LDAP group for user [" + userDn + "]", e);
        }
        return groups;
    }

    /**
     * Fetches the groups from the user attributes (if supported).  This method could later be abstracted out
     * into a strategy class
     *
     * @param userDn User fully distinguished name to fetch group membership from
     * @return list of groups the user is a member of.
     */
    public List<String> getGroupsFromUserAttrs(String userDn) {
        List<String> groupDns = new LinkedList<>();
        try {
            Attributes results = jndiContext.getAttributes(userDn, new String[] { groupAttribute });
            for (NamingEnumeration ae = results.getAll(); ae.hasMore(); ) {
                Attribute attr = (Attribute) ae.next();
                for (NamingEnumeration attrEnum = attr.getAll(); attrEnum.hasMore(); ) {
                    Object val = attrEnum.next();
                    if (val instanceof String) {
                        String stringVal = (String) val;
                        groupDns.add(stringVal);
                    }
                }
            }
        } catch (NamingException e) {
            throw new LdapException("Could not look up group attributes for user [" + userDn + "]", e);
        }
        return groupDns;
    }
}
