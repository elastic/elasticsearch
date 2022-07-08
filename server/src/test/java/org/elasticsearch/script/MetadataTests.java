/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;

public class MetadataTests extends ESTestCase {
    public void testDefaultValidatorForAllMetadata() {
        for (IngestDocument.Metadata m : IngestDocument.Metadata.values()) {
            assertThat(Metadata.VALIDATORS, hasEntry(equalTo(m.getFieldName()), notNullValue()));
        }
        assertEquals(IngestDocument.Metadata.values().length, Metadata.VALIDATORS.size());
    }
}
