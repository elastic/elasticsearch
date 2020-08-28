/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.apache.commons.codec.binary.Hex;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ActiveDirectorySIDUtilTests extends ESTestCase {

    private static final String USER_SID_HEX ="01050000000000051500000050bd51b583ef8ebc4c75521ae9030000";
    private static final String USER_STRING_SID = "S-1-5-21-3042032976-3163484035-441611596-1001";

    public void testSidConversion() throws Exception {
        assertThat(USER_STRING_SID, equalTo(ActiveDirectorySIDUtil.convertToString(Hex.decodeHex(USER_SID_HEX.toCharArray()))));
    }
}
