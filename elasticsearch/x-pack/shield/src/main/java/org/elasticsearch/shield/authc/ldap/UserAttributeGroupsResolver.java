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
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.ldap.support.LdapSession.GroupsResolver;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.OBJECT_CLASS_PRESENCE_FILTER;
import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.searchForEntry;

/**
*
*/
class UserAttributeGroupsResolver implements GroupsResolver {

    private final String attribute;

    public UserAttributeGroupsResolver(Settings settings) {
        this(settings.get("user_group_attribute", "memberOf"));
    }

    public UserAttributeGroupsResolver(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public List<String> resolve(LDAPInterface connection, String userDn, TimeValue timeout, ESLogger logger) {
        try {
            SearchRequest request = new SearchRequest(userDn, SearchScope.BASE, OBJECT_CLASS_PRESENCE_FILTER, attribute);
            request.setTimeLimitSeconds(Math.toIntExact(timeout.seconds()));
            SearchResultEntry result = searchForEntry(connection, request, logger);
            Attribute attributeReturned = result.getAttribute(attribute);
            if (attributeReturned == null) {
                return Collections.emptyList();
            }
            String[] values = attributeReturned.getValues();
            return Arrays.asList(values);
        } catch (LDAPException e) {
            throw new ElasticsearchException("could not look up group attributes for DN [{}]", e, userDn);
        }
    }
}
