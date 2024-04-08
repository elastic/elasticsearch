/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.time.DateTimeException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DataDescriptionTests extends AbstractXContentSerializingTestCase<DataDescription> {

    public void testDefault() {
        DataDescription dataDescription = new DataDescription.Builder().build();
        assertThat(dataDescription.getTimeField(), equalTo("time"));
        assertThat(dataDescription.getTimeFormat(), equalTo("epoch_ms"));
    }

    public void testVerify_GivenValidFormat() {
        DataDescription.Builder description = new DataDescription.Builder();
        description.setTimeFormat("epoch");
        description.setTimeFormat("epoch_ms");
        description.setTimeFormat("yyyy-MM-dd HH");
        description.setTimeFormat("yyyy.MM.dd G 'at' HH:mm:ss z");
    }

    public void testVerify_GivenInValidFormat() {
        DataDescription.Builder description = new DataDescription.Builder();
        expectThrows(IllegalArgumentException.class, () -> description.setTimeFormat(null));

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> description.setTimeFormat("invalid"));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_INVALID_TIMEFORMAT, "invalid"), e.getMessage());

        e = expectThrows(ElasticsearchException.class, () -> description.setTimeFormat(""));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_INVALID_TIMEFORMAT, ""), e.getMessage());

        e = expectThrows(ElasticsearchException.class, () -> description.setTimeFormat("y-M-dd"));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_INVALID_TIMEFORMAT, "y-M-dd"), e.getMessage());
        expectThrows(ElasticsearchException.class, () -> description.setTimeFormat("YYY-mm-UU hh:mm:ssY"));

        Throwable cause = e.getCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(DateTimeException.class));
    }

    public void testIsTransformTime_GivenTimeFormatIsEpoch() {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setTimeFormat("epoch");
        assertFalse(dd.build().isTransformTime());
    }

    public void testIsTransformTime_GivenTimeFormatIsEpochMs() {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setTimeFormat("epoch_ms");
        assertTrue(dd.build().isTransformTime());
    }

    public void testIsTransformTime_GivenTimeFormatPattern() {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setTimeFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        assertTrue(dd.build().isTransformTime());
    }

    public void testInvalidDataFormat() throws Exception {
        BytesArray json = new BytesArray("{ \"format\":\"INEXISTENT_FORMAT\" }");
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            json.streamInput()
        );
        XContentParseException ex = expectThrows(XContentParseException.class, () -> DataDescription.STRICT_PARSER.apply(parser, null));
        assertThat(ex.getMessage(), containsString("[data_description] failed to parse field [format]"));
        Throwable cause = ex.getCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(
            cause.getMessage(),
            containsString("No enum constant org.elasticsearch.xpack.core.ml.job.config.DataDescription.DataFormat.INEXISTENT_FORMAT")
        );
    }

    @Override
    protected DataDescription createTestInstance() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        if (randomBoolean()) {
            dataDescription.setTimeField(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            String format;
            if (randomBoolean()) {
                format = DataDescription.EPOCH;
            } else if (randomBoolean()) {
                format = DataDescription.EPOCH_MS;
            } else {
                format = "yyyy-MM-dd HH:mm:ss.SSS";
            }
            dataDescription.setTimeFormat(format);
        }
        return dataDescription.build();
    }

    @Override
    protected Reader<DataDescription> instanceReader() {
        return DataDescription::new;
    }

    @Override
    protected DataDescription doParseInstance(XContentParser parser) {
        return DataDescription.STRICT_PARSER.apply(parser, null).build();
    }

    @Override
    protected DataDescription mutateInstance(DataDescription instance) {
        String timeField = instance.getTimeField();
        String timeFormat = instance.getTimeFormat();
        switch (between(0, 1)) {
            case 0 -> timeField += randomAlphaOfLengthBetween(1, 10);
            case 1 -> timeFormat = "yyyy-MM-dd-HH-mm-ss";
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DataDescription(timeField, timeFormat);
    }
}
