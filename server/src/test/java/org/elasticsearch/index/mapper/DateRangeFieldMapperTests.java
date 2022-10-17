/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class DateRangeFieldMapperTests extends RangeFieldMapperTests {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis";
    private static final String FROM_DATE = "2016-10-31";
    private static final String TO_DATE = "2016-11-01 20:00:00";

    @Override
    protected XContentBuilder rangeSource(XContentBuilder in) throws IOException {
        return in.startObject("field").field("gt", FROM_DATE).field("lt", TO_DATE).endObject();
    }

    @Override
    protected String storedValue() {
        return "1477872000000";
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "date_range");
        b.field("format", DATE_FORMAT);
    }

    @Override
    protected boolean supportsCoerce() {
        return false;
    }

    @Override
    protected Object rangeValue() {
        return "1477872000000";
    }

    public void testIllegalFormatField() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "date_range").array("format", "test_format")))
        );
        assertThat(e.getMessage(), containsString("Invalid format: [[test_format]]: Unknown pattern letter: t"));
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
