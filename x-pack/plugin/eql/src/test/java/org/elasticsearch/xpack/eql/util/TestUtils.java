/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.Types;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class TestUtils {

    public static final Configuration TEST_CFG = new Configuration(DateUtils.UTC, Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null, Mode.PLAIN,
            null, null, null, false, false);

    
    private TestUtils() {
    }

    public static Map<String, EsField> loadMapping(String name) {
        InputStream stream = TestUtils.class.getResourceAsStream("/" + name);
        assertNotNull("Could not find mapping resource:" + name, stream);
        return Types.fromEs(XContentHelper.convertToMap(JsonXContent.jsonXContent, stream, ESTestCase.randomBoolean()));
    }
}
