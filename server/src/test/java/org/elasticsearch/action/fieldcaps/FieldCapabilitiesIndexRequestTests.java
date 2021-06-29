/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesIndexRequestTests extends ESTestCase {

    public void testSerializingWithRuntimeFieldsBeforeSupportedThrows() {
        FieldCapabilitiesIndexRequest request = new FieldCapabilitiesIndexRequest(
            new String[] { "field" },
            "index",
            new OriginalIndices(new String[] { "original_index" }, IndicesOptions.LENIENT_EXPAND_OPEN),
            null,
            0L,
            singletonMap("day_of_week", singletonMap("type", "keyword"))
        );
        Version v = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, VersionUtils.getPreviousVersion(Version.V_7_12_0));
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> copyWriteable(request, writableRegistry(), FieldCapabilitiesRequest::new, v)
        );
        assertThat(e.getMessage(), equalTo("Versions before 7.12.0 don't support [runtime_mappings], but trying to send _field_caps "
            + "request to a node with version [" + v + "]"));
    }
}
