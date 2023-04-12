/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LongFieldMapperTests extends WholeNumberFieldMapperTests {

    @Override
    protected Number missingValue() {
        return 123L;
    }

    @Override
    protected List<OutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "9223372036854775808", "out of range for a long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "1e999999999", "out of range for a long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "-9223372036854775809", "out of range for a long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "-1e999999999", "out of range for a long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, new BigInteger("9223372036854775808"), "out of range of long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, new BigInteger("-9223372036854775809"), "out of range of long")
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "long");
    }

    @Override
    protected boolean allowsIndexTimeScript() {
        return true;
    }

    public void testScriptAndPrecludedParameters() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "long");
                b.field("script", "test");
                b.field("coerce", "true");
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: Field [coerce] cannot be set in conjunction with field [script]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "long");
                b.field("script", "test");
                b.field("null_value", 7);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [null_value] cannot be set in conjunction with field [script]")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "long");
                b.field("script", "test");
                b.field("ignore_malformed", "true");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [ignore_malformed] cannot be set in conjunction with field [script]")
            );
        }
    }

    public void testLongIndexingOutOfRange() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long").field("ignore_malformed", true)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.rawField("field", new BytesArray("9223372036854775808").streamInput(), XContentType.JSON))
        );
        assertThat(doc.rootDoc().getFields("field"), empty());
    }

    public void testLongIndexingCoercesIntoRange() throws Exception {
        // the following two strings are in-range for a long after coercion
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "9223372036854775807.9")));
        assertThat(doc.rootDoc().getFields("field"), hasSize(1));
        doc = mapper.parse(source(b -> b.field("field", "-9223372036854775808.9")));
        assertThat(doc.rootDoc().getFields("field"), hasSize(1));
    }

    @Override
    protected Number randomNumber() {
        if (randomBoolean()) {
            return randomLong();
        }
        if (randomBoolean()) {
            return randomDouble();
        }
        assumeFalse("https://github.com/elastic/elasticsearch/issues/70585", true);
        return randomDoubleBetween(Long.MIN_VALUE, Long.MAX_VALUE, true);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/70585")
    public void testFetchCoerced() throws IOException {
        assertFetch(randomFetchTestMapper(), "field", 3.783147882954537E18, randomFetchTestFormat());
    }

    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            @SuppressWarnings("unchecked")
            protected <T> T compileOtherScript(Script script, ScriptContext<T> context) {
                if (context == LongFieldScript.CONTEXT) {
                    return (T) LongFieldScript.PARSE_FROM_SOURCE;
                }
                throw new UnsupportedOperationException("Unknown script " + script.getIdOrCode());
            }

            @Override
            protected LongFieldScript.Factory emptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new LongFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {}
                };
            }

            @Override
            protected LongFieldScript.Factory nonEmptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new LongFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {
                        emit(1);
                    }
                };
            }
        };
    }
}
