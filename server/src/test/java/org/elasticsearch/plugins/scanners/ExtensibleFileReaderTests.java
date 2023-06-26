/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ExtensibleFileReaderTests extends ESTestCase {

    public void testLoadingFromFile() {
        ExtensibleFileReader extensibleFileReader = new ExtensibleFileReader("/test_extensible.json");

        Map<String, String> stringStringMap = extensibleFileReader.readFromFile();
        assertThat(
            stringStringMap,
            equalTo(
                Map.of(
                    "org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleClass",
                    "org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleClass",
                    "org.elasticsearch.plugins.scanners.extensible_test_classes.SubClass",
                    "org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleClass",
                    "org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleInterface",
                    "org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleInterface"
                )
            )
        );
    }
}
