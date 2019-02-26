/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AutodetectControlMsgWriterTests extends ESTestCase {
    private LengthEncodedWriter lengthEncodedWriter;

    @Before
    public void setUpMocks() {
        lengthEncodedWriter = Mockito.mock(LengthEncodedWriter.class);
    }

    public void testWriteFlushControlMessage_GivenAdvanceTime() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder().advanceTime("1234567890").build();

        writer.writeFlushControlMessage(flushJobParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("t1234567890");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushControlMessage_GivenSkipTime() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder().skipTime("1234567890").build();

        writer.writeFlushControlMessage(flushJobParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("s1234567890");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushControlMessage_GivenSkipAndAdvanceTime() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder().skipTime("1000").advanceTime("2000").build();

        writer.writeFlushControlMessage(flushJobParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeField("s1000");
        inOrder.verify(lengthEncodedWriter).writeField("t2000");
    }

    public void testWriteFlushControlMessage_GivenCalcInterimResultsWithNoTimeParams() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder()
                .calcInterim(true).build();

        writer.writeFlushControlMessage(flushJobParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("i");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushControlMessage_GivenPlainFlush() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder().build();

        writer.writeFlushControlMessage(flushJobParams);

        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushControlMessage_GivenCalcInterimResultsWithTimeParams() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder()
                .calcInterim(true)
                .forTimeRange(TimeRange.builder().startTime("120").endTime("180").build())
                .build();

        writer.writeFlushControlMessage(flushJobParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("i120 180");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushControlMessage_GivenCalcInterimAndAdvanceTime() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder()
                .calcInterim(true)
                .forTimeRange(TimeRange.builder().startTime("50").endTime("100").build())
                .advanceTime("180")
                .build();

        writer.writeFlushControlMessage(flushJobParams);

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
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);
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
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);

        writer.writeResetBucketsMessage(
                new DataLoadParams(TimeRange.builder().startTime("0").endTime("600").build(), Optional.empty()));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("r0 600");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateModelPlotMessage() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);

        writer.writeUpdateModelPlotMessage(new ModelPlotConfig(true, "foo,bar"));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[modelPlotConfig]\nboundspercentile = 95.0\nterms = foo,bar\n");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateDetectorRulesMessage() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 4);

        DetectionRule rule1 = new DetectionRule.Builder(createRule(5)).build();
        DetectionRule rule2 = new DetectionRule.Builder(createRule(5)).build();
        writer.writeUpdateDetectorRulesMessage(2, Arrays.asList(rule1, rule2));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[detectorRules]\ndetectorIndex=2\n" +
                "rulesJson=[{\"actions\":[\"skip_result\"],\"conditions\":" +
                "[{\"applies_to\":\"actual\",\"operator\":\"gt\",\"value\":5.0}]}," +
                "{\"actions\":[\"skip_result\"],\"conditions\":[" +
                "{\"applies_to\":\"actual\",\"operator\":\"gt\",\"value\":5.0}]}]");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateFiltersMessage() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 2);

        MlFilter filter1 = MlFilter.builder("filter_1").setItems("a").build();
        MlFilter filter2 = MlFilter.builder("filter_2").setItems("b", "c").build();

        writer.writeUpdateFiltersMessage(Arrays.asList(filter1, filter2));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter, times(1)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[filters]\nfilter.filter_1 = [\"a\"]\nfilter.filter_2 = [\"b\",\"c\"]\n");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateScheduledEventsMessage() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 2);

        ScheduledEvent.Builder event1 = new ScheduledEvent.Builder();
        event1.calendarId("moon");
        event1.description("new year");
        event1.startTime(ZonedDateTime.parse("2018-01-01T00:00:00Z").toInstant());
        event1.endTime(ZonedDateTime.parse("2018-01-02T00:00:00Z").toInstant());

        ScheduledEvent.Builder event2 = new ScheduledEvent.Builder();
        event2.calendarId("moon");
        event2.description("Jan maintenance day");
        event2.startTime(ZonedDateTime.parse("2018-01-06T00:00:00Z").toInstant());
        event2.endTime(ZonedDateTime.parse("2018-01-07T00:00:00Z").toInstant());

        writer.writeUpdateScheduledEventsMessage(Arrays.asList(event1.build(), event2.build()), TimeValue.timeValueHours(1));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter, times(1)).writeField("");
        ArgumentCaptor<String> capturedMessage = ArgumentCaptor.forClass(String.class);
        inOrder.verify(lengthEncodedWriter).writeField(capturedMessage.capture());
        assertThat(capturedMessage.getValue(), equalTo("u[scheduledEvents]\n"
                + "scheduledevent.0.description = new year\n"
                + "scheduledevent.0.rules = [{\"actions\":[\"skip_result\",\"skip_model_update\"],"
                +     "\"conditions\":[{\"applies_to\":\"time\",\"operator\":\"gte\",\"value\":1.5147648E9},"
                +     "{\"applies_to\":\"time\",\"operator\":\"lt\",\"value\":1.5148512E9}]}]\n"
                + "scheduledevent.1.description = Jan maintenance day\n"
                + "scheduledevent.1.rules = [{\"actions\":[\"skip_result\",\"skip_model_update\"],"
                +     "\"conditions\":[{\"applies_to\":\"time\",\"operator\":\"gte\",\"value\":1.5151968E9},"
                +     "{\"applies_to\":\"time\",\"operator\":\"lt\",\"value\":1.5152832E9}]}]\n"));
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateScheduledEventsMessage_GivenEmpty() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 2);

        writer.writeUpdateScheduledEventsMessage(Collections.emptyList(), TimeValue.timeValueHours(1));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter, times(1)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[scheduledEvents]\nclear = true\n");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteStartBackgroundPersistMessage() throws IOException {
        AutodetectControlMsgWriter writer = new AutodetectControlMsgWriter(lengthEncodedWriter, 2);
        writer.writeStartBackgroundPersistMessage();

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("w");

        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter).writeField("");
        StringBuilder spaces = new StringBuilder();
        IntStream.rangeClosed(1, AutodetectControlMsgWriter.FLUSH_SPACES_LENGTH).forEach(i -> spaces.append(' '));
        inOrder.verify(lengthEncodedWriter).writeField(spaces.toString());
        inOrder.verify(lengthEncodedWriter).flush();

        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    private static List<RuleCondition> createRule(double value) {
        return Collections.singletonList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, value));
    }
}
