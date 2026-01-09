/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

public class BulkRequestParserLengthPrefixedTests extends BulkRequestParserTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(
            new Object[] { XContentType.JSON },
            new Object[] { XContentType.SMILE },
            new Object[] { XContentType.CBOR },
            new Object[] { XContentType.YAML }
        );
    }

    private final XContentType contentType;

    public BulkRequestParserLengthPrefixedTests(XContentType contentType) {
        this.contentType = contentType;
    }

    protected XContentType contentType() {
        return contentType;
    }

    protected RestBulkAction.BulkFormat bulkFormat() {
        return RestBulkAction.BulkFormat.PREFIX_LENGTH;
    }

    public void testHandleWrongLength() throws IOException {
        BytesArray doc1 = convertToFormat(new BytesArray(" { \"index\":{ \"require_alias\": false } }"));
        BytesArray doc2 = convertToFormat(new BytesArray(" { \"field\": \"value\" }"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeInt(doc1.length());
            out.write(doc1.array(), doc1.arrayOffset(), doc1.length());
            out.writeInt(randomValueOtherThanMany(i -> i == doc2.length(), ESTestCase::randomInt));
            out.write(doc2.array(), doc2.arrayOffset(), doc2.length());
            BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
            expectThrows(
                IllegalArgumentException.class,
                () -> parser.parse(
                    out.bytes(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    contentType(),
                    bulkFormat(),
                    (r, t) -> fail(),
                    req -> fail(),
                    req -> fail()
                )
            );
        }
    }
}
