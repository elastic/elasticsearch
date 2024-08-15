/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class XContentGeneratorTests extends ESTestCase {
    public void testCopyCurrentStructureRoundtripWithDefaultParsers() throws IOException {
        for (var xContentType : new XContentType[] { XContentType.JSON, XContentType.SMILE, XContentType.YAML, XContentType.CBOR }) {
            roundtripTest(xContentType, (input) -> xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, input));
        }
    }

    // This test mimics real world scenario when parsers are decorators of other parsers.
    // Behavior is different due to instanceof check inside JsonXContentGenerator#copyCurrentStructure.
    public void testCopyCurrentStructureRoundtripWithWrappedParsers() throws IOException {
        for (var xContentType : new XContentType[] { XContentType.JSON, XContentType.SMILE, XContentType.YAML, XContentType.CBOR }) {
            roundtripTest(xContentType, (input) -> {
                var defaultParser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, input);
                return new FilterXContentParser() {
                    @Override
                    protected XContentParser delegate() {
                        return defaultParser;
                    }
                };
            });
        }
    }

    private void roundtripTest(XContentType xContentType, CheckedFunction<StreamInput, XContentParser, IOException> parserCreator)
        throws IOException {
        var inputData = XContentBuilder.builder(xContentType.xContent());
        inputData.startObject();

        inputData.nullField("null");
        inputData.field("str", "hey");
        inputData.field("boolean", false);
        inputData.field("int", 420);
        inputData.field("long", 420L);
        inputData.field("float", 42.0f);
        inputData.field("double", 42.0);
        inputData.field("big_integer", BigInteger.valueOf(420));
        inputData.field("big_decimal", BigDecimal.valueOf(42.0));
        inputData.startArray("arr").value(1).value(2).endArray();

        inputData.endObject();

        inputData.close();

        var inputBytes = BytesReference.bytes(inputData);
        var parser = parserCreator.apply(inputBytes.streamInput());
        assertNull(parser.currentToken());

        var output = new ByteArrayOutputStream();

        var sut = xContentType.xContent().createGenerator(output);
        sut.copyCurrentStructure(parser);
        sut.close();

        assertArrayEquals(inputBytes.array(), output.toByteArray());
    }
}
