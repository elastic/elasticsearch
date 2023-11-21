/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.junit.rules.ExternalResource;

public class OpenLdapTestContainer extends ExternalResource {

    public String getAddress() {
        return "ldaps://127.0.0.1.ip.es.io:123";
    }
}
