/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.IntStream;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.junit.Before;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;

public class ControlMsgToProcessWriterTests extends ESTestCase {
    private LengthEncodedWriter lengthEncodedWriter;

    @Before
    public void setUpMocks() {
        lengthEncodedWriter = Mockito.mock(LengthEncodedWriter.class);
    }

    public void testWriteCalcInterimMessage_GivenAdvanceTime() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);
        InterimResultsParams interimResultsParams = InterimResultsParams.builder()
                .advanceTime("1234567890").build();

        writer.writeCalcInterimMessage(interimResultsParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("t1234567890");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteCalcInterimMessage_GivenCalcInterimResultsWithNoTimeParams() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);
        InterimResultsParams interimResultsParams = InterimResultsParams.builder()
                .calcInterim(true).build();

        writer.writeCalcInterimMessage(interimResultsParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("i");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteCalcInterimMessage_GivenNeitherCalcInterimNorAdvanceTime() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);
        InterimResultsParams interimResultsParams = InterimResultsParams.builder().build();

        writer.writeCalcInterimMessage(interimResultsParams);

        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteCalcInterimMessage_GivenCalcInterimResultsWithTimeParams() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);
        InterimResultsParams interimResultsParams = InterimResultsParams.builder()
                .calcInterim(true)
                .forTimeRange(TimeRange.builder().startTime("120").endTime("180").build())
                .build();

        writer.writeCalcInterimMessage(interimResultsParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("i120 180");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteCalcInterimMessage_GivenCalcInterimAndAdvanceTime() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);
        InterimResultsParams interimResultsParams = InterimResultsParams.builder()
                .calcInterim(true)
                .forTimeRange(TimeRange.builder().startTime("50").endTime("100").build())
                .advanceTime("180")
                .build();

        writer.writeCalcInterimMessage(interimResultsParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("t180");
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("i50 100");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushMessage() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);
        long firstId = Long.parseLong(writer.writeFlushMessage());
        Mockito.reset(lengthEncodedWriter);

        writer.writeFlushMessage();

        InOrder inOrder = inOrder(lengthEncodedWriter);

        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("f" + (firstId + 1));

        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        StringBuilder spaces = new StringBuilder();
        IntStream.rangeClosed(1, 8192).forEach(i -> spaces.append(' '));
        inOrder.verify(lengthEncodedWriter).writeField(spaces.toString());

        inOrder.verify(lengthEncodedWriter).flush();
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteResetBucketsMessage() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);

        writer.writeResetBucketsMessage(
                new DataLoadParams(TimeRange.builder().startTime("0").endTime("600").build(), false, Optional.empty()));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("r0 600");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }
}
