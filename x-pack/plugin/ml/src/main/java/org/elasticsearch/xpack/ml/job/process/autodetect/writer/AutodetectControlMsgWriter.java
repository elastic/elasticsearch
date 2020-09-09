/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.ml.process.writer.AbstractControlMsgWriter;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer for sending control messages to the C++ autodetect process.
 * The data written to outputIndex is length encoded.
 */
public class AutodetectControlMsgWriter extends AbstractControlMsgWriter {

    /**
     * This must match the code defined in the api::CFieldDataCategorizer C++ class.
     */
    private static final String CATEGORIZATION_STOP_ON_WARN_MESSAGE_CODE = "c";

    /**
     * This must match the code defined in the api::CAnomalyJob C++ class.
     */
    private static final String FLUSH_MESSAGE_CODE = "f";

    /**
     * This must match the code defined in the api::CAnomalyJob C++ class.
     */
    private static final String INTERIM_MESSAGE_CODE = "i";

    /**
     * This must match the code defined in the api::CAnomalyJob C++ class.
     */
    private static final String FORECAST_MESSAGE_CODE = "p";

    /**
     * This must match the code defined in the api::CAnomalyJob C++ class.
     */
    public static final String RESET_BUCKETS_MESSAGE_CODE = "r";

    /**
     * This must match the code defined in the api::CAnomalyJob C++ class.
     */
    private static final String ADVANCE_TIME_MESSAGE_CODE = "t";

    /**
     * This must match the code defined in the api::CAnomalyJob C++ class.
     */
    private static final String SKIP_TIME_MESSAGE_CODE = "s";

    /**
     * This must match the code defined in the api::CAnomalyJob C++ class.
     */
    public static final String UPDATE_MESSAGE_CODE = "u";

    /**
     * This must match the code defined in the api::CAnomalyJob C++ class.
     */
    public static final String BACKGROUND_PERSIST_MESSAGE_CODE = "w";

    /**
     * An number to uniquely identify each flush so that subsequent code can
     * wait for acknowledgement of the correct flush.
     */
    private static AtomicLong ms_FlushNumber = new AtomicLong(1);

    /**
     * Construct the control message writer with a LengthEncodedWriter
     *
     * @param lengthEncodedWriter The writer
     * @param numberOfFields      The number of fields the process expects in each record
     */
    public AutodetectControlMsgWriter(LengthEncodedWriter lengthEncodedWriter, int numberOfFields) {
        super(lengthEncodedWriter, numberOfFields);
    }

    /**
     * Create the control message writer with a OutputStream. A
     * LengthEncodedWriter is created on the OutputStream parameter
     *
     * @param os             The output stream
     * @param numberOfFields The number of fields the process expects in each record
     */
    public static AutodetectControlMsgWriter create(OutputStream os, int numberOfFields) {
        return new AutodetectControlMsgWriter(new LengthEncodedWriter(os), numberOfFields);
    }

    /**
     * Writes the control messages that are requested when flushing a job.
     * Those control messages need to be followed by a flush message in order
     * for them to reach the C++ process immediately. List of supported controls:
     *
     * <ul>
     *   <li>advance time</li>
     *   <li>calculate interim results</li>
     * </ul>
     *
     * @param params Parameters describing the controls that will accompany the flushing
     *               (e.g. calculating interim results, time control, etc.)
     */
    public void writeFlushControlMessage(FlushJobParams params) throws IOException {
        if (params.shouldSkipTime()) {
            writeMessage(SKIP_TIME_MESSAGE_CODE + params.getSkipTime());
        }
        if (params.shouldAdvanceTime()) {
            writeMessage(ADVANCE_TIME_MESSAGE_CODE + params.getAdvanceTime());
        }
        if (params.shouldCalculateInterim()) {
            writeControlCodeFollowedByTimeRange(INTERIM_MESSAGE_CODE, params.getStart(), params.getEnd());
        }
    }

    /**
     * Send a flush message to the C++ autodetect process.
     * This actually consists of two messages: one to carry the flush ID and the
     * other (which might not be processed until much later) to fill the buffers
     * and force prior messages through.
     *
     * @return an ID for this flush that will be echoed back by the C++
     * autodetect process once it is complete.
     */
    public String writeFlushMessage() throws IOException {
        String flushId = Long.toString(ms_FlushNumber.getAndIncrement());
        writeMessage(FLUSH_MESSAGE_CODE + flushId);

        fillCommandBuffer();

        lengthEncodedWriter.flush();
        return flushId;
    }

    public void writeForecastMessage(ForecastParams params) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("forecast_id", params.getForecastId());
        builder.field("create_time", params.getCreateTime());

        if (params.getDuration() != 0) {
            builder.field("duration", params.getDuration());
        }
        if (params.getExpiresIn() != -1) {
            builder.field("expires_in", params.getExpiresIn());
        }
        if (params.getTmpStorage() != null) {
            builder.field("tmp_storage", params.getTmpStorage());
        }
        if (params.getMaxModelMemory() != null) {
            builder.field("max_model_memory", params.getMaxModelMemory());
        }
        builder.endObject();

        writeMessage(FORECAST_MESSAGE_CODE + Strings.toString(builder));
        fillCommandBuffer();
        lengthEncodedWriter.flush();
    }

    public void writeResetBucketsMessage(DataLoadParams params) throws IOException {
        writeControlCodeFollowedByTimeRange(RESET_BUCKETS_MESSAGE_CODE, params.getStart(), params.getEnd());
    }

    private void writeControlCodeFollowedByTimeRange(String code, String start, String end)
            throws IOException {
        StringBuilder message = new StringBuilder(code);
        if (start.isEmpty() == false) {
            message.append(start);
            message.append(' ');
            message.append(end);
        }
        writeMessage(message.toString());
    }

    public void writeUpdateModelPlotMessage(ModelPlotConfig modelPlotConfig) throws IOException {
        StringWriter configWriter = new StringWriter();
        configWriter.append(UPDATE_MESSAGE_CODE).append("[modelPlotConfig]\n");
        new ModelPlotConfigWriter(modelPlotConfig, configWriter).write();
        writeMessage(configWriter.toString());
    }

    public void writeCategorizationStopOnWarnMessage(boolean isStopOnWarn) throws IOException {
        writeMessage(CATEGORIZATION_STOP_ON_WARN_MESSAGE_CODE + isStopOnWarn);
    }

    public void writeUpdateDetectorRulesMessage(int detectorIndex, List<DetectionRule> rules) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(UPDATE_MESSAGE_CODE).append("[detectorRules]\n");
        stringBuilder.append("detectorIndex=").append(Integer.toString(detectorIndex)).append("\n");

        stringBuilder.append("rulesJson=");

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startArray();
            for (DetectionRule rule : rules) {
                rule.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endArray();
            stringBuilder.append(Strings.toString(builder));
        }

        writeMessage(stringBuilder.toString());
    }

    public void writeUpdateFiltersMessage(List<MlFilter> filters) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(UPDATE_MESSAGE_CODE).append("[filters]\n");
        new MlFilterWriter(filters, stringBuilder).write();
        writeMessage(stringBuilder.toString());
    }

    public void writeUpdateScheduledEventsMessage(List<ScheduledEvent> events, TimeValue bucketSpan) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(UPDATE_MESSAGE_CODE).append("[scheduledEvents]\n");
        if (events.isEmpty()) {
            stringBuilder.append("clear = true\n");
        } else {
            new ScheduledEventsWriter(events, bucketSpan, stringBuilder).write();
        }
        writeMessage(stringBuilder.toString());
    }

    public void writeStartBackgroundPersistMessage() throws IOException {
        writeMessage(BACKGROUND_PERSIST_MESSAGE_CODE);
        fillCommandBuffer();
        lengthEncodedWriter.flush();
    }
}
