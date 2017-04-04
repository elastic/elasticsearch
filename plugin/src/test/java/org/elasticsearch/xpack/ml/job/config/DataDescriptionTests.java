/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ml.job.config.DataDescription.DataFormat;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DataDescriptionTests extends AbstractSerializingTestCase<DataDescription> {

    public void testDefault() {
        DataDescription dataDescription = new DataDescription.Builder().build();
        assertThat(dataDescription.getFormat(), equalTo(DataFormat.XCONTENT));
        assertThat(dataDescription.getTimeField(), equalTo("time"));
        assertThat(dataDescription.getTimeFormat(), equalTo("epoch_ms"));
        assertThat(dataDescription.getFieldDelimiter(), is(nullValue()));
        assertThat(dataDescription.getQuoteCharacter(), is(nullValue()));
    }

    public void testDefaultDelimited() {
        DataDescription.Builder dataDescriptionBuilder = new DataDescription.Builder();
        dataDescriptionBuilder.setFormat(DataFormat.DELIMITED);
        DataDescription dataDescription = dataDescriptionBuilder.build();

        assertThat(dataDescription.getFormat(), equalTo(DataFormat.DELIMITED));
        assertThat(dataDescription.getTimeField(), equalTo("time"));
        assertThat(dataDescription.getTimeFormat(), equalTo("epoch_ms"));
        assertThat(dataDescription.getFieldDelimiter(), is('\t'));
        assertThat(dataDescription.getQuoteCharacter(), is('"'));
    }

    public void testVerify_GivenValidFormat() {
        DataDescription.Builder description = new DataDescription.Builder();
        description.setTimeFormat("epoch");
        description.setTimeFormat("epoch_ms");
        description.setTimeFormat("yyyy-MM-dd HH");
        String goodFormat = "yyyy.MM.dd G 'at' HH:mm:ss z";
        description.setTimeFormat(goodFormat);
    }

    public void testVerify_GivenInValidFormat() {
        DataDescription.Builder description = new DataDescription.Builder();
        expectThrows(IllegalArgumentException.class, () -> description.setTimeFormat(null));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> description.setTimeFormat("invalid"));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_INVALID_TIMEFORMAT, "invalid"), e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> description.setTimeFormat(""));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_INVALID_TIMEFORMAT, ""), e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> description.setTimeFormat("y-M-dd"));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_INVALID_TIMEFORMAT, "y-M-dd"), e.getMessage());
        expectThrows(IllegalArgumentException.class, () -> description.setTimeFormat("YYY-mm-UU hh:mm:ssY"));
    }

    public void testTransform_GivenDelimitedAndEpoch() {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setFormat(DataFormat.DELIMITED);
        dd.setTimeFormat("epoch");
        assertFalse(dd.build().transform());
    }

    public void testTransform_GivenDelimitedAndEpochMs() {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setFormat(DataFormat.DELIMITED);
        dd.setTimeFormat("epoch_ms");
        assertTrue(dd.build().transform());
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

    public void testEquals_GivenDifferentDateFormat() {
        DataDescription.Builder description1 = new DataDescription.Builder();
        description1.setFormat(DataFormat.XCONTENT);
        description1.setQuoteCharacter('"');
        description1.setTimeField("timestamp");
        description1.setTimeFormat("epoch");
        description1.setFieldDelimiter(',');

        DataDescription.Builder description2 = new DataDescription.Builder();
        description2.setFormat(DataFormat.DELIMITED);
        description2.setQuoteCharacter('"');
        description2.setTimeField("timestamp");
        description2.setTimeFormat("epoch");
        description2.setFieldDelimiter(',');

        assertFalse(description1.build().equals(description2.build()));
        assertFalse(description2.build().equals(description1.build()));
    }

    public void testEquals_GivenDifferentQuoteCharacter() {
        DataDescription.Builder description1 = new DataDescription.Builder();
        description1.setFormat(DataFormat.XCONTENT);
        description1.setQuoteCharacter('"');
        description1.setTimeField("timestamp");
        description1.setTimeFormat("epoch");
        description1.setFieldDelimiter(',');

        DataDescription.Builder description2 = new DataDescription.Builder();
        description2.setFormat(DataFormat.XCONTENT);
        description2.setQuoteCharacter('\'');
        description2.setTimeField("timestamp");
        description2.setTimeFormat("epoch");
        description2.setFieldDelimiter(',');

        assertFalse(description1.build().equals(description2.build()));
        assertFalse(description2.build().equals(description1.build()));
    }

    public void testEquals_GivenDifferentTimeField() {
        DataDescription.Builder description1 = new DataDescription.Builder();
        description1.setFormat(DataFormat.XCONTENT);
        description1.setQuoteCharacter('"');
        description1.setTimeField("timestamp");
        description1.setTimeFormat("epoch");
        description1.setFieldDelimiter(',');

        DataDescription.Builder description2 = new DataDescription.Builder();
        description2.setFormat(DataFormat.XCONTENT);
        description2.setQuoteCharacter('"');
        description2.setTimeField("time");
        description2.setTimeFormat("epoch");
        description2.setFieldDelimiter(',');

        assertFalse(description1.build().equals(description2.build()));
        assertFalse(description2.build().equals(description1.build()));
    }

    public void testEquals_GivenDifferentTimeFormat() {
        DataDescription.Builder description1 = new DataDescription.Builder();
        description1.setFormat(DataFormat.XCONTENT);
        description1.setQuoteCharacter('"');
        description1.setTimeField("timestamp");
        description1.setTimeFormat("epoch");
        description1.setFieldDelimiter(',');

        DataDescription.Builder description2 = new DataDescription.Builder();
        description2.setFormat(DataFormat.XCONTENT);
        description2.setQuoteCharacter('"');
        description2.setTimeField("timestamp");
        description2.setTimeFormat("epoch_ms");
        description2.setFieldDelimiter(',');

        assertFalse(description1.build().equals(description2.build()));
        assertFalse(description2.build().equals(description1.build()));
    }

    public void testEquals_GivenDifferentFieldDelimiter() {
        DataDescription.Builder description1 = new DataDescription.Builder();
        description1.setFormat(DataFormat.XCONTENT);
        description1.setQuoteCharacter('"');
        description1.setTimeField("timestamp");
        description1.setTimeFormat("epoch");
        description1.setFieldDelimiter(',');

        DataDescription.Builder description2 = new DataDescription.Builder();
        description2.setFormat(DataFormat.XCONTENT);
        description2.setQuoteCharacter('"');
        description2.setTimeField("timestamp");
        description2.setTimeFormat("epoch");
        description2.setFieldDelimiter(';');

        assertFalse(description1.build().equals(description2.build()));
        assertFalse(description2.build().equals(description1.build()));
    }

    public void testInvalidDataFormat() throws Exception {
        BytesArray json = new BytesArray("{ \"format\":\"INEXISTENT_FORMAT\" }");
        XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, json);
        ParsingException ex = expectThrows(ParsingException.class,
                () -> DataDescription.PARSER.apply(parser, null));
        assertThat(ex.getMessage(), containsString("[data_description] failed to parse field [format]"));
        Throwable cause = ex.getCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(cause.getMessage(),
                containsString("No enum constant org.elasticsearch.xpack.ml.job.config.DataDescription.DataFormat.INEXISTENT_FORMAT"));
    }

    public void testInvalidFieldDelimiter() throws Exception {
        BytesArray json = new BytesArray("{ \"field_delimiter\":\",,\" }");
        XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, json);
        ParsingException ex = expectThrows(ParsingException.class,
                () -> DataDescription.PARSER.apply(parser, null));
        assertThat(ex.getMessage(), containsString("[data_description] failed to parse field [field_delimiter]"));
        Throwable cause = ex.getCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(cause.getMessage(),
                containsString("String must be a single character, found [,,]"));
    }

    public void testInvalidQuoteCharacter() throws Exception {
        BytesArray json = new BytesArray("{ \"quote_character\":\"''\" }");
        XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, json);
        ParsingException ex = expectThrows(ParsingException.class,
                () -> DataDescription.PARSER.apply(parser, null));
        assertThat(ex.getMessage(), containsString("[data_description] failed to parse field [quote_character]"));
        Throwable cause = ex.getCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(cause.getMessage(), containsString("String must be a single character, found ['']"));
    }

    @Override
    protected DataDescription createTestInstance() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        if (randomBoolean()) {
            dataDescription.setFormat(randomFrom(DataFormat.values()));
        }
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
                format = "yyy.MM.dd G 'at' HH:mm:ss z";
            }
            dataDescription.setTimeFormat(format);
        }
        if (randomBoolean()) {
            dataDescription.setFieldDelimiter(randomAlphaOfLength(1).charAt(0));
        }
        if (randomBoolean()) {
            dataDescription.setQuoteCharacter(randomAlphaOfLength(1).charAt(0));
        }
        return dataDescription.build();
    }

    @Override
    protected Reader<DataDescription> instanceReader() {
        return DataDescription::new;
    }

    @Override
    protected DataDescription parseInstance(XContentParser parser) {
        return DataDescription.PARSER.apply(parser, null).build();
    }
}
