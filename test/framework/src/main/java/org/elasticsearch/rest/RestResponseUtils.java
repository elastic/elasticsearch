/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;

import static org.elasticsearch.test.ESTestCase.asInstanceOf;
import static org.elasticsearch.test.ESTestCase.fail;
import static org.elasticsearch.transport.BytesRefRecycler.NON_RECYCLING_INSTANCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class RestResponseUtils {
    private RestResponseUtils() {}

    public static BytesReference getBodyContent(RestResponse restResponse) {
        if (restResponse.isChunked() == false) {
            return restResponse.content();
        }

        final var firstResponseBodyPart = restResponse.chunkedContent();
        assert firstResponseBodyPart.isPartComplete() == false;

        final int pageSize;
        try (var page = NON_RECYCLING_INSTANCE.obtain()) {
            pageSize = page.v().length;
        }

        try (var out = new BytesStreamOutput()) {
            while (firstResponseBodyPart.isPartComplete() == false) {
                try (var chunk = firstResponseBodyPart.encodeChunk(pageSize, NON_RECYCLING_INSTANCE)) {
                    chunk.writeTo(out);
                }
            }
            assert firstResponseBodyPart.isLastPart() : "RestResponseUtils#getBodyContent does not support continuations (yet)";

            out.flush();
            return out.bytes();
        } catch (Exception e) {
            return fail(e);
        }
    }

    public static String getTextBodyContent(Iterator<CheckedConsumer<Writer, IOException>> iterator) {
        try (var writer = new StringWriter()) {
            while (iterator.hasNext()) {
                iterator.next().accept(writer);
            }
            writer.flush();
            return writer.toString();
        } catch (Exception e) {
            return fail(e);
        }
    }

    public static <T extends ToXContent> T setUpXContentMock(T mock) {
        try {
            when(mock.toXContent(any(), any())).thenAnswer(
                invocation -> asInstanceOf(XContentBuilder.class, invocation.getArgument(0)).startObject().endObject()
            );
        } catch (IOException e) {
            fail(e);
        }
        return mock;
    }
}
