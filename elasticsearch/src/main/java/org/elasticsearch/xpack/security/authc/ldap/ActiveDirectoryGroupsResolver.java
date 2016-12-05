/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.OBJECT_CLASS_PRESENCE_FILTER;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.search;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.searchForEntry;

class ActiveDirectoryGroupsResolver implements GroupsResolver {

    private static final String TOKEN_GROUPS = "tokenGroups";
    private final String baseDn;
    private final LdapSearchScope scope;

    ActiveDirectoryGroupsResolver(Settings settings, String baseDnDefault) {
        this.baseDn = settings.get("base_dn", baseDnDefault);
        this.scope = LdapSearchScope.resolve(settings.get("scope"), LdapSearchScope.SUB_TREE);
    }

    @Override
    public void resolve(LDAPInterface connection, String userDn, TimeValue timeout, Logger logger, Collection<Attribute> attributes,
                        ActionListener<List<String>> listener) {
        buildGroupQuery(connection, userDn, timeout,
                ActionListener.wrap((filter) -> {
                    if (filter == null) {
                        listener.onResponse(Collections.emptyList());
                    } else {
                        logger.debug("group SID to DN [{}] search filter: [{}]", userDn, filter);
                        search(connection, baseDn, scope.scope(), filter, Math.toIntExact(timeout.seconds()),
                                ActionListener.wrap((results) -> {
                                            List<String> groups = results.stream()
                                                    .map(SearchResultEntry::getDN)
                                                    .collect(Collectors.toList());
                                            listener.onResponse(Collections.unmodifiableList(groups));
                                        },
                                        listener::onFailure),
                                SearchRequest.NO_ATTRIBUTES);
                    }
                }, listener::onFailure));
    }

    @Override
    public String[] attributes() {
        // we have to return null since the tokenGroups attribute is computed and can only be retrieved using a BASE level search
        return null;
    }

    static void buildGroupQuery(LDAPInterface connection, String userDn, TimeValue timeout, ActionListener<Filter> listener) {
        searchForEntry(connection, userDn, SearchScope.BASE, OBJECT_CLASS_PRESENCE_FILTER, Math.toIntExact(timeout.seconds()),
                ActionListener.wrap((entry) -> {
                    if (entry == null || entry.hasAttribute(TOKEN_GROUPS) == false) {
                        listener.onResponse(null);
                    } else {
                        final byte[][] tokenGroupSIDBytes = entry.getAttributeValueByteArrays(TOKEN_GROUPS);
                        List<Filter> orFilters = Arrays.stream(tokenGroupSIDBytes)
                                .map((sidBytes) -> Filter.createEqualityFilter("objectSid", binarySidToStringSid(sidBytes)))
                                .collect(Collectors.toList());
                        listener.onResponse(Filter.createORFilter(orFilters));
                    }
                }, listener::onFailure),
                TOKEN_GROUPS);
    }

    /**
     * To better understand what the sid is and how its string representation looks like, see
     * http://blogs.msdn.com/b/alextch/archive/2007/06/18/sample-java-application-that-retrieves-group-membership-of-an-active-directory
     * -user-account.aspx
     *
     * @param SID byte encoded security ID
     */
    private static String binarySidToStringSid(byte[] SID) {
        String strSID;

        //convert the SID into string format

        long version;
        long authority;
        long count;
        long rid;

        strSID = "S";
        version = SID[0];
        strSID = strSID + "-" + Long.toString(version);
        authority = SID[4];

        for (int i = 0; i < 4; i++) {
            authority <<= 8;
            authority += SID[4 + i] & 0xFF;
        }

        strSID = strSID + "-" + Long.toString(authority);
        count = SID[2];
        count <<= 8;
        count += SID[1] & 0xFF;
        for (int j = 0; j < count; j++) {
            rid = SID[11 + (j * 4)] & 0xFF;
            for (int k = 1; k < 4; k++) {
                rid <<= 8;
                rid += SID[11 - k + (j * 4)] & 0xFF;
            }
            strSID = strSID + "-" + Long.toString(rid);
        }
        return strSID;
    }

}
