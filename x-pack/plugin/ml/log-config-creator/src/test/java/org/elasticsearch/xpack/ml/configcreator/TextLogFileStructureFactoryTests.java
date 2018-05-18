/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.junit.Before;

import java.io.IOException;

public class TextLogFileStructureFactoryTests extends LogConfigCreatorTestCase {

    private LogFileStructureFactory factory;

    @Before
    public void setup() throws IOException {
        factory = new TextLogFileStructureFactory(TEST_TERMINAL, null);
    }

    // No need to check JSON, XML, CSV or TSV because they come earlier in the order we check formats

    public void testCanCreateFromSampleGivenText() {

        assertTrue(factory.canCreateFromSample(TEXT_SAMPLE));
    }
}
