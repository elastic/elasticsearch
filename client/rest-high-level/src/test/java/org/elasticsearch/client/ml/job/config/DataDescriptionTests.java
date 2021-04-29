/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import static org.elasticsearch.client.ml.job.config.DataDescription.DataFormat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DataDescriptionTests extends AbstractXContentTestCase<DataDescription> {

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
                format = "yyyy-MM-dd HH:mm:ss.SSS";
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
    protected DataDescription doParseInstance(XContentParser parser) {
        return DataDescription.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
