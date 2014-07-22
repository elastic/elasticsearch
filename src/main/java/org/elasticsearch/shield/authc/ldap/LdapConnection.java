/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates jndi/ldap functionality into one authenticated connection.  The constructor is package scoped, assuming
 * instances of this connection will be produced by the LdapConnectionFactory.bind() methods.
 *
 * A standard looking usage pattern could look like this:
     <pre>
     try (LdapConnection session = ldapFac.bindXXX(...);
     ...do stuff with the session
     }
     </pre>
 */
public class LdapConnection implements Closeable {

    private final String bindDn;
    private final DirContext ldapContext;

    private final String groupSearchDN;
    private final boolean isGroupSubTreeSearch;
    private final boolean isFindGroupsByAttribute;


    /**
     * This object is intended to be constructed by the LdapConnectionFactory
     */
    LdapConnection(DirContext ctx, String boundName, boolean isFindGroupsByAttribute, boolean isGroupSubTreeSearch, String groupSearchDN) {
        this.ldapContext = ctx;
        this.bindDn = boundName;
        this.isGroupSubTreeSearch = isGroupSubTreeSearch;
        this.groupSearchDN = groupSearchDN;
        this.isFindGroupsByAttribute = isFindGroupsByAttribute;
    }

    /**
     * LDAP connections should be closed to clean up resources.  However, the jndi contexts have the finalize
     * implemented properly so that it will clean up on garbage collection.
     */
    public void close(){
        try {
            ldapContext.close();
        } catch (NamingException e) {
            throw new SecurityException("Could not close the LDAP connection", e);
        }
    }

    /**
     * Fetches the groups that the user is a member of
     * @return List of group membership
     */
    public List<String> getGroups(){
        return isFindGroupsByAttribute ? getGroupsFromUserAttrs(bindDn) : getGroupsFromSearch(bindDn);
    }

    /**
     * Fetches the groups of a user by doing a search.  This could be abstracted out into a strategy class or through
     * an inherited class (with getGroups as the template method).
     * @param userDn user fully distinguished name to fetch group membership for
     * @return fully distinguished names of the roles
     */
    List<String> getGroupsFromSearch(String userDn){
        List<String> groups = new LinkedList<>();
        SearchControls search = new SearchControls();
        search.setReturningAttributes( new String[0] );
        search.setSearchScope( this.isGroupSubTreeSearch ? SearchControls.SUBTREE_SCOPE : SearchControls.ONELEVEL_SCOPE);

        //This could be made could be made configurable but it should cover all cases
        String filter = "(&" +
                "(|(objectclass=groupOfNames)(objectclass=groupOfUniqueNames)(objectclass=group)) " +
                "(|(uniqueMember={0})(member={0})))";

        try {
            NamingEnumeration<SearchResult> results = ldapContext.search(
                    groupSearchDN, filter, new Object[]{userDn}, search);
            while (results.hasMoreElements()){
                groups.add(results.next().getNameInNamespace());
            }
        } catch (NamingException e) {
            throw new SecurityException("Could not search for an LDAP group for user [" + userDn + "]", e);
        }
        return groups;
    }

    /**
     * Fetches the groups from the user attributes (if supported).  This method could later be abstracted out
     * into a strategy class
     * @param userDn User fully distinguished name to fetch group membership from
     * @return list of groups the user is a member of.
     */
    List<String> getGroupsFromUserAttrs(String userDn) {
        List<String> groupDns = new LinkedList<>();
        try {
            Attributes results = ldapContext.getAttributes(userDn, new String[]{"memberOf", "isMemberOf"});
            for(NamingEnumeration ae = results.getAll(); ae.hasMore();) {
                Attribute attr = (Attribute)ae.next();
                for (NamingEnumeration attrEnum = attr.getAll(); attrEnum.hasMore();) {
                    Object val = attrEnum.next();
                    if (val instanceof String) {
                        String stringVal = (String) val;
                        groupDns.add(stringVal);
                    }
                }
            }
        } catch (NamingException e) {
            throw new SecurityException("Could not look up group attributes for user [" + userDn + "]", e);
        }
        return groupDns;
    }

    /**
     * Fetches common user attributes from the user.  Its a good way to ensure a connection works.
     */
    public Map<String,String[]> getUserAttrs(String userDn) {
        Map <String, String[]>userAttrs = new HashMap<>();
        try {
            Attributes results = ldapContext.getAttributes(userDn, new String[]{"uid", "memberOf", "isMemberOf"});
            for(NamingEnumeration ae = results.getAll(); ae.hasMore();) {
                Attribute attr = (Attribute)ae.next();
                LinkedList<String> attrList = new LinkedList<>();
                for (NamingEnumeration attrEnum = attr.getAll(); attrEnum.hasMore();) {
                    Object val = attrEnum.next();
                    if (val instanceof String) {
                        String stringVal = (String) val;
                        attrList.add(stringVal);
                    }
                }
                String[] attrArray = attrList.toArray(new String[attrList.size()]);
                userAttrs.put(attr.getID(), attrArray);
            }
        } catch (NamingException e) {
            throw new SecurityException("Could not look up attributes for user [" + userDn + "]", e);
        }
        return userAttrs;
    }

    public String getAuthenticatedUserDn() {
        return bindDn;
    }
}
