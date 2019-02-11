/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql;

import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.util.DateUtils;

public class TestUtils {

    private TestUtils() {}

    public static final Configuration TEST_CFG = new Configuration(DateUtils.UTC, Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null, Mode.PLAIN,
            null, null, null);

}
