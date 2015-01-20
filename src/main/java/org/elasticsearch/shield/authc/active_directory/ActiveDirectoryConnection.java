/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapConnection;

import javax.naming.directory.DirContext;

/**
 * An Ldap Connection customized for active directory.
 */
public class ActiveDirectoryConnection extends AbstractLdapConnection {

    /**
     * This object is intended to be constructed by the LdapConnectionFactory
     */
    ActiveDirectoryConnection(ESLogger logger, DirContext ctx, String boundName, GroupsResolver resolver, TimeValue timeout) {
        super(logger, ctx, boundName, resolver, timeout);
    }

}
