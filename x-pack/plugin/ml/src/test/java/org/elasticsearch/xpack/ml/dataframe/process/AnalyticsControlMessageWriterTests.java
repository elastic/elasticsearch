/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;
import org.junit.Before;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.stream.IntStream;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AnalyticsControlMessageWriterTests extends ESTestCase {

    private LengthEncodedWriter lengthEncodedWriter;

    @Before
    public void setUpMocks() {
        lengthEncodedWriter = Mockito.mock(LengthEncodedWriter.class);
    }

    public void testWriteEndOfData() throws IOException {
        AnalyticsControlMessageWriter writer = new AnalyticsControlMessageWriter(lengthEncodedWriter, 4);

        writer.writeEndOfData();

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("$");

        StringBuilder spaces = new StringBuilder();
        IntStream.rangeClosed(1, 2048).forEach(i -> spaces.append(' '));
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField(spaces.toString());

        inOrder.verify(lengthEncodedWriter).flush();

        verifyNoMoreInteractions(lengthEncodedWriter);
    }
}
