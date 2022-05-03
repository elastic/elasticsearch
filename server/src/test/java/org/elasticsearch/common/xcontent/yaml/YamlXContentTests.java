/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.yaml;

import org.elasticsearch.common.xcontent.BaseXContentTestCase;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.ByteArrayOutputStream;

public class YamlXContentTests extends BaseXContentTestCase {

    @Override
    public XContentType xcontentType() {
        return XContentType.YAML;
    }

    public void testBigInteger() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        XContentGenerator generator = YamlXContent.yamlXContent.createGenerator(os);
        doTestBigInteger(generator, os);
    }
}
