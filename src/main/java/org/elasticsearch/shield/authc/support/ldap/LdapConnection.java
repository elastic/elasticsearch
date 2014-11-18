/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import java.io.Closeable;
import java.util.List;

/**
 * Represents a LDAP connection with an authenticated/bound user that needs closing.
 */
public interface LdapConnection extends Closeable {
    void close();

    List<String> getGroups();

    String getAuthenticatedUserDn();
}
