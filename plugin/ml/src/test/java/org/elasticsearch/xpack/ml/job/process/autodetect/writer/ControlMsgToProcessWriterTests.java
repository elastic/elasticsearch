/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.Condition;
import org.elasticsearch.xpack.core.ml.job.config.Connective;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.config.RuleConditionType;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.junit.Before;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ControlMsgToProcessWriterTests extends ESTestCase {
    private LengthEncodedWriter lengthEncodedWriter;

    @Before
    public void setUpMocks() {
        lengthEncodedWriter = Mockito.mock(LengthEncodedWriter.class);
    }

    public void testWriteFlushControlMessage_GivenAdvanceTime() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder().advanceTime("1234567890").build();

        writer.writeFlushControlMessage(flushJobParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("t1234567890");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushControlMessage_GivenSkipTime() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder().skipTime("1234567890").build();

        writer.writeFlushControlMessage(flushJobParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("s1234567890");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushControlMessage_GivenSkipAndAdvanceTime() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder().skipTime("1000").advanceTime("2000").build();

        writer.writeFlushControlMessage(flushJobParams);

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeField("s1000");
        inOrder.verify(lengthEncodedWriter).writeField("t2000");
    }

    public void testWriteFlushControlMessage_GivenCalcInterimResultsWithNoTimeParams() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);
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
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);
        FlushJobParams flushJobParams = FlushJobParams.builder().build();

        writer.writeFlushControlMessage(flushJobParams);

        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteFlushControlMessage_GivenCalcInterimResultsWithTimeParams() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);
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
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);
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
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);
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
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);

        writer.writeResetBucketsMessage(
                new DataLoadParams(TimeRange.builder().startTime("0").endTime("600").build(), Optional.empty()));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("r0 600");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateModelPlotMessage() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);

        writer.writeUpdateModelPlotMessage(new ModelPlotConfig(true, "foo,bar"));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[modelPlotConfig]\nboundspercentile = 95.0\nterms = foo,bar\n");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateDetectorRulesMessage() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 4);

        DetectionRule rule1 = new DetectionRule.Builder(createRule("5")).setTargetFieldName("targetField1")
                .setTargetFieldValue("targetValue").setConditionsConnective(Connective.AND).build();
        DetectionRule rule2 = new DetectionRule.Builder(createRule("5")).setTargetFieldName("targetField2")
                .setTargetFieldValue("targetValue").setConditionsConnective(Connective.AND).build();
        writer.writeUpdateDetectorRulesMessage(2, Arrays.asList(rule1, rule2));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(4);
        inOrder.verify(lengthEncodedWriter, times(3)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[detectorRules]\ndetectorIndex=2\n" +
                            "rulesJson=[{\"actions\":[\"filter_results\"],\"conditions_connective\":\"and\",\"conditions\":" +
                "[{\"type\":\"numerical_actual\",\"condition\":{\"operator\":\"gt\",\"value\":\"5\"}}]," +
                "\"target_field_name\":\"targetField1\",\"target_field_value\":\"targetValue\"}," +
                "{\"actions\":[\"filter_results\"],\"conditions_connective\":\"and\",\"conditions\":[" +
                "{\"type\":\"numerical_actual\",\"condition\":{\"operator\":\"gt\",\"value\":\"5\"}}]," +
                "\"target_field_name\":\"targetField2\",\"target_field_value\":\"targetValue\"}]");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateFiltersMessage() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);

        MlFilter filter1 = new MlFilter("filter_1", Arrays.asList("a"));
        MlFilter filter2 = new MlFilter("filter_2", Arrays.asList("b", "c"));

        writer.writeUpdateFiltersMessage(Arrays.asList(filter1, filter2));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter, times(1)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[filters]\nfilter.filter_1 = [\"a\"]\nfilter.filter_2 = [\"b\",\"c\"]\n");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateScheduledEventsMessage() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);

        ScheduledEvent.Builder event1 = new ScheduledEvent.Builder();
        event1.calendarId("moon");
        event1.description("new year");
        event1.startTime(ZonedDateTime.parse("2018-01-01T00:00:00Z"));
        event1.endTime(ZonedDateTime.parse("2018-01-02T00:00:00Z"));

        ScheduledEvent.Builder event2 = new ScheduledEvent.Builder();
        event2.calendarId("moon");
        event2.description("Jan maintenance day");
        event2.startTime(ZonedDateTime.parse("2018-01-06T00:00:00Z"));
        event2.endTime(ZonedDateTime.parse("2018-01-07T00:00:00Z"));

        writer.writeUpdateScheduledEventsMessage(Arrays.asList(event1.build(), event2.build()), TimeValue.timeValueHours(1));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter, times(1)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[scheduledEvents]\n"
                + "scheduledevent.0.description = new year\n"
                + "scheduledevent.0.rules = [{\"actions\":[\"filter_results\",\"skip_sampling\"],\"conditions_connective\":\"and\","
                +     "\"conditions\":[{\"type\":\"time\",\"condition\":{\"operator\":\"gte\",\"value\":\"1514764800\"}},"
                +     "{\"type\":\"time\",\"condition\":{\"operator\":\"lt\",\"value\":\"1514851200\"}}]}]\n"
                + "scheduledevent.1.description = Jan maintenance day\n"
                + "scheduledevent.1.rules = [{\"actions\":[\"filter_results\",\"skip_sampling\"],\"conditions_connective\":\"and\","
                +     "\"conditions\":[{\"type\":\"time\",\"condition\":{\"operator\":\"gte\",\"value\":\"1515196800\"}},"
                +     "{\"type\":\"time\",\"condition\":{\"operator\":\"lt\",\"value\":\"1515283200\"}}]}]\n"
        );
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteUpdateScheduledEventsMessage_GivenEmpty() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);

        writer.writeUpdateScheduledEventsMessage(Collections.emptyList(), TimeValue.timeValueHours(1));

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter, times(1)).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("u[scheduledEvents]\nclear = true\n");
        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    public void testWriteStartBackgroundPersistMessage() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(lengthEncodedWriter, 2);
        writer.writeStartBackgroundPersistMessage();

        InOrder inOrder = inOrder(lengthEncodedWriter);
        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter).writeField("");
        inOrder.verify(lengthEncodedWriter).writeField("w");

        inOrder.verify(lengthEncodedWriter).writeNumFields(2);
        inOrder.verify(lengthEncodedWriter).writeField("");
        StringBuilder spaces = new StringBuilder();
        IntStream.rangeClosed(1, ControlMsgToProcessWriter.FLUSH_SPACES_LENGTH).forEach(i -> spaces.append(' '));
        inOrder.verify(lengthEncodedWriter).writeField(spaces.toString());
        inOrder.verify(lengthEncodedWriter).flush();

        verifyNoMoreInteractions(lengthEncodedWriter);
    }

    private static List<RuleCondition> createRule(String value) {
        Condition condition = new Condition(Operator.GT, value);
        return Collections.singletonList(RuleCondition.createNumerical(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition));
    }
}
