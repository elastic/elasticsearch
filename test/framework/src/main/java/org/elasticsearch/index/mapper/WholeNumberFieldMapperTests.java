/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.IndexMode;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public abstract class WholeNumberFieldMapperTests extends NumberFieldMapperTests {

    protected void testDecimalCoerce() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "7.89")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        IndexableField pointField = fields.get(0);
        assertEquals(7, pointField.numericValue().doubleValue(), 0d);
    }

    @Override
    public void testDimension() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        NumberFieldMapper.NumberFieldType ft = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());

        assertDimension(true, NumberFieldMapper.NumberFieldType::isDimension);
        assertDimension(false, NumberFieldMapper.NumberFieldType::isDimension);
    }

    public void testDimensionIndexedAndDocvalues() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", true).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", true);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
    }

    public void testDimensionMultiValuedFieldTSDB() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }), IndexMode.TIME_SERIES);

        ParsedDocument doc = mapper.parse(source(null, b -> {
            b.array("field", randomNumber(), randomNumber(), randomNumber());
            b.field("@timestamp", Instant.now());
        }, TimeSeriesRoutingHashFieldMapper.encode(randomInt())));
        assertThat(doc.docs().get(0).getFields("field"), hasSize(greaterThan(1)));
    }

    public void testDimensionMultiValuedFieldNonTSDB() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }), randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.array("field", randomNumber(), randomNumber(), randomNumber());
            b.field("@timestamp", Instant.now());
        }));
        assertThat(doc.docs().get(0).getFields("field"), hasSize(greaterThan(1)));
    }

    public void testMetricAndDimension() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", "counter").field("time_series_dimension", true);
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString("Field [time_series_dimension] cannot be set in conjunction with field [time_series_metric]")
        );
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        super.registerParameters(checker);

        // dimension cannot be updated
        registerDimensionChecks(checker);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new NumberSyntheticSourceSupport(Number::longValue, ignoreMalformed);
    }
}
