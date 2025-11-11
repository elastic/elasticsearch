/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;

public class CCMIndexTests extends ESTestCase {
    public void testMappings() throws IOException {
        assertThat(Strings.toString(CCMIndex.mappings()), is(XContentHelper.stripWhitespace("""
            {
                "_doc": {
                    "_meta": {
                        "managed_index_mappings_version": 1
                    },
                    "dynamic": "strict",
                    "properties": {
                        "api_key": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """)));
    }
}
