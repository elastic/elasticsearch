/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.elasticsearch.common.time.DateFormatter;

import java.io.IOException;
import java.util.Collections;

public class TimestampMetadataMapper extends MetadataFieldMapper {

    public static final String CONTENT_TYPE = "@timestamp";
    public static final String NAME = "@timestamp";
    public static final DateFormatter DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");

    private TimestampMetadataMapper() {
        super(
            new DateFieldMapper.DateFieldType(
                NAME,
                true,
                false,
                true,
                DEFAULT_DATE_TIME_FORMATTER,
                DateFieldMapper.Resolution.MILLISECONDS,
                null,
                null,
                Collections.emptyMap()
            )
        );
    }

    private static final TimestampMetadataMapper INSTANCE = new TimestampMetadataMapper();

    public static TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {
        if (context.getTimestampValue() != null) {
            throw new MapperParsingException("@timestamp field can only have a single value");
        }
        long value = DEFAULT_DATE_TIME_FORMATTER.parseMillis(context.parser().text());
        context.setTimestampValue(value);
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        if (context.getTimestampValue() == null) {
            throw new MapperParsingException("Document did not contain a @timestamp field");
        }
        context.rootDoc().add(new LongPoint(NAME, context.getTimestampValue()));
        context.rootDoc().add(new SortedNumericDocValuesField(NAME, context.getTimestampValue()));
    }
}
