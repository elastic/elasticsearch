/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.enrich.EnrichPlugin.FlatNumberOrByteSizeValue;

public class FlatNumberOrByteSizeValueTests extends ESTestCase {

    private static final String SETTING_NAME = "test.setting";

    public void testParse() {
        int number = randomIntBetween(1, Integer.MAX_VALUE);
        assertEquals(
            new FlatNumberOrByteSizeValue((long) number),
            FlatNumberOrByteSizeValue.parse(Integer.toString(number), SETTING_NAME, null)
        );
        assertEquals(
            new FlatNumberOrByteSizeValue(ByteSizeValue.ofGb(number)),
            FlatNumberOrByteSizeValue.parse(number + "GB", SETTING_NAME, null)
        );
        assertEquals(
            new FlatNumberOrByteSizeValue(ByteSizeValue.ofGb(number)),
            FlatNumberOrByteSizeValue.parse(number + "g", SETTING_NAME, null)
        );
        int percentage = randomIntBetween(0, 100);
        assertEquals(
            new FlatNumberOrByteSizeValue(
                ByteSizeValue.ofBytes((long) ((double) percentage / 100 * JvmInfo.jvmInfo().getConfiguredMaxHeapSize()))
            ),
            FlatNumberOrByteSizeValue.parse(percentage + "%", SETTING_NAME, null)
        );
        assertEquals(new FlatNumberOrByteSizeValue(0L), FlatNumberOrByteSizeValue.parse("0", SETTING_NAME, null));
        assertEquals(new FlatNumberOrByteSizeValue(ByteSizeValue.ZERO), FlatNumberOrByteSizeValue.parse("0GB", SETTING_NAME, null));
        assertEquals(new FlatNumberOrByteSizeValue(ByteSizeValue.ZERO), FlatNumberOrByteSizeValue.parse("0%", SETTING_NAME, null));
        // Assert default value.
        assertEquals(
            new FlatNumberOrByteSizeValue((long) number),
            FlatNumberOrByteSizeValue.parse(null, SETTING_NAME, new FlatNumberOrByteSizeValue((long) number))
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
