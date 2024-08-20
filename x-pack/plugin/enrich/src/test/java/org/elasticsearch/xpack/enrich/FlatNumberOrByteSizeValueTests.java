/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.enrich.EnrichPlugin.FlatNumberOrByteSizeValue;

public class FlatNumberOrByteSizeValueTests extends ESTestCase {

    private static final String SETTING_NAME = "test.setting";

    public void testParse() {
        assertEquals(new FlatNumberOrByteSizeValue(7L), FlatNumberOrByteSizeValue.parse("7", SETTING_NAME, null));
        assertEquals(new FlatNumberOrByteSizeValue(ByteSizeValue.ofGb(2)), FlatNumberOrByteSizeValue.parse("2GB", SETTING_NAME, null));
        assertEquals(
            new FlatNumberOrByteSizeValue(ByteSizeValue.ofBytes((long) (0.05 * JvmInfo.jvmInfo().getConfiguredMaxHeapSize()))),
            FlatNumberOrByteSizeValue.parse("5%", SETTING_NAME, null)
        );
        assertEquals(
            new FlatNumberOrByteSizeValue(3L),
            FlatNumberOrByteSizeValue.parse(null, SETTING_NAME, new FlatNumberOrByteSizeValue(3L))
        );
        assertThrows(ElasticsearchParseException.class, () -> FlatNumberOrByteSizeValue.parse("5GB%", SETTING_NAME, null));
        assertThrows(ElasticsearchParseException.class, () -> FlatNumberOrByteSizeValue.parse("5%GB", SETTING_NAME, null));
        assertThrows(ElasticsearchParseException.class, () -> FlatNumberOrByteSizeValue.parse("5GBX", SETTING_NAME, null));
    }

    private void assertEquals(FlatNumberOrByteSizeValue expected, FlatNumberOrByteSizeValue actual) {
        assertEquals(expected.byteSizeValue(), actual.byteSizeValue());
        assertEquals(expected.flatNumber(), actual.flatNumber());
    }
}
