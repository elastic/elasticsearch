/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.io.File;
import java.net.URISyntaxException;
import java.util.Hashtable;

public class ADGroups	{
    public static void main (String[] args) throws URISyntaxException {
        LdapSslSocketFactory.init(ImmutableSettings.builder()
                .put("shield.authc.ldap.truststore", new File(LdapConnectionTests.class.getResource("ldaptrust.jks").toURI()))
                .build());

        Hashtable env = new Hashtable();
        String adminName = "CN=Tony Stark,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        String adminPassword = "NickFuryHeartsES";
        String ldapURL = "ldaps://ad.test.elasticsearch.com:636";
        //set security credentials, note using simple cleartext authentication
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, adminName);
        env.put(Context.SECURITY_CREDENTIALS, adminPassword);
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put("java.naming.ldap.factory.socket", LdapSslSocketFactory.class.getName());

        //connect to my domain controller
        env.put(Context.PROVIDER_URL, ldapURL);
        //specify attributes to be returned in binary format
        env.put("java.naming.ldap.attributes.binary", "tokenGroups");


        try {
            //Create the initial directory context
            LdapContext ctx = new InitialLdapContext(env,null);

            //Create the search controls
            SearchControls userSearchCtls = new SearchControls();

            //Specify the search scope
            userSearchCtls.setSearchScope(SearchControls.OBJECT_SCOPE);

            //specify the LDAP search filter to find the user in question
            String userSearchFilter = "(objectClass=user)";

            //paceholder for an LDAP filter that will store SIDs of the groups the user belongs to
            StringBuffer groupsSearchFilter = new StringBuffer();
            groupsSearchFilter.append("(|");

            //Specify the Base for the search
            String userSearchBase = "CN=Tony Stark,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";

            //Specify the attributes to return
            String userReturnedAtts[] = { "tokenGroups", "CN" };
            userSearchCtls.setReturningAttributes(userReturnedAtts);

            //Search for objects using the filter
            NamingEnumeration userAnswer = ctx.search(userSearchBase, userSearchFilter, userSearchCtls);

            //Loop through the search results
            while (userAnswer.hasMoreElements()) {

                SearchResult sr = (SearchResult)userAnswer.next();
                Attributes attrs = sr.getAttributes();

                if (attrs != null) {
                    try {
                        for (NamingEnumeration ae = attrs.getAll();ae.hasMore();) {
                            Attribute attr = (Attribute)ae.next();
                            for (NamingEnumeration e = attr.getAll();e.hasMore();) {

                                byte[] sid = (byte[])e.next();
                                groupsSearchFilter.append("(objectSid=" + binarySidToStringSid(sid) + ")");

                            }
                            groupsSearchFilter.append(")");
                        }

                    }
                    catch (NamingException e) {
                        System.err.println("Problem listing membership: " + e);
                    }
                }
            }


            // Search for groups the user belongs to in order to get their names
            //Create the search controls
            SearchControls groupsSearchCtls = new SearchControls();

            //Specify the search scope
            groupsSearchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            //Specify the Base for the search
            String groupsSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";

            //Specify the attributes to return
            String groupsReturnedAtts[]={};
            groupsSearchCtls.setReturningAttributes(groupsReturnedAtts);

            //Search for objects using the filter
            NamingEnumeration groupsAnswer = ctx.search(groupsSearchBase, groupsSearchFilter.toString(), groupsSearchCtls);

            //Loop through the search results
            while (groupsAnswer.hasMoreElements()) {
                SearchResult sr = (SearchResult)groupsAnswer.next();
                System.out.println(sr.getNameInNamespace());
            }

            ctx.close();

        }

        catch (NamingException e) {
            System.err.println("Problem searching directory: " + e);
        }
    }


    public static final String binarySidToStringSid( byte[] SID ) {

        String strSID = "";

        //convert the SID into string format

        long version;
        long authority;
        long count;
        long rid;

        strSID = "S";
        version = SID[0];
        strSID = strSID + "-" + Long.toString(version);
        authority = SID[4];

        for (int i = 0;i<4;i++) {
            authority <<= 8;
            authority += SID[4+i] & 0xFF;
        }

        strSID = strSID + "-" + Long.toString(authority);
        count = SID[2];
        count <<= 8;
        count += SID[1] & 0xFF;
        for (int j=0;j<count;j++) {
            rid = SID[11 + (j*4)] & 0xFF;
            for (int k=1;k<4;k++) {
                rid <<= 8;
                rid += SID[11-k + (j*4)] & 0xFF;
            }
            strSID = strSID + "-" + Long.toString(rid);
        }
        return strSID;
    }

}
