/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DoubleFieldMapperTests extends NumberFieldMapperTests {

    @Override
    protected Number missingValue() {
        return 123d;
    }

    @Override
    protected List<NumberTypeOutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            NumberTypeOutOfRangeSpec.of(
                NumberFieldMapper.NumberType.DOUBLE,
                "1.7976931348623157E309",
                "[double] supports only finite values"
            ),
            NumberTypeOutOfRangeSpec.of(
                NumberFieldMapper.NumberType.DOUBLE,
                "-1.7976931348623157E309",
                "[double] supports only finite values"
            ),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.DOUBLE, Double.NaN, "[double] supports only finite values"),
            NumberTypeOutOfRangeSpec.of(
                NumberFieldMapper.NumberType.DOUBLE,
                Double.POSITIVE_INFINITY,
                "[double] supports only finite values"
            ),
            NumberTypeOutOfRangeSpec.of(
                NumberFieldMapper.NumberType.DOUBLE,
                Double.NEGATIVE_INFINITY,
                "[double] supports only finite values"
            )
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "double");
    }

    @Override
    protected boolean allowsIndexTimeScript() {
        return true;
    }

    @Override
    protected Number randomNumber() {
        /*
         * The source parser and doc values round trip will both increase
         * the precision to 64 bits if the value is less precise.
         * randomDoubleBetween will smear the values out across a wide
         * range of valid values.
         */
        return randomBoolean() ? randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true) : randomFloat();
    }

    public void testScriptAndPrecludedParameters() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "double");
                b.field("script", "test");
                b.field("coerce", "true");
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: Field [coerce] cannot be set in conjunction with field [script]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "double");
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
                b.field("type", "double");
                b.field("script", "test");
                b.field("ignore_malformed", "true");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [ignore_malformed] cannot be set in conjunction with field [script]")
            );
        }
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            @SuppressWarnings("unchecked")
            protected <T> T compileOtherScript(Script script, ScriptContext<T> context) {
                if (context == DoubleFieldScript.CONTEXT) {
                    return (T) DoubleFieldScript.PARSE_FROM_SOURCE;
                }
                throw new UnsupportedOperationException("Unknown script " + script.getIdOrCode());
            }

            @Override
            protected DoubleFieldScript.Factory emptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new DoubleFieldScript(
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
            protected DoubleFieldScript.Factory nonEmptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new DoubleFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {
                        emit(1.0);
                    }
                };
            }
        };
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new NumberSyntheticSourceSupport(Number::doubleValue, ignoreMalformed);
    }

    protected SyntheticSourceSupport syntheticSourceSupportForKeepTests(boolean ignoreMalformed, Mapper.SourceKeepMode sourceKeepMode) {
        return new NumberSyntheticSourceSupportForKeepTests(Number::doubleValue, ignoreMalformed, sourceKeepMode);
    }
}
