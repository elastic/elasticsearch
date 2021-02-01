/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process.logging;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * Provide access to the C++ log messages that arrive via a named pipe in JSON format.
 */
public class CppLogMessage implements ToXContentObject, Writeable {
    /**
     * Field Names (these are defined by log4cxx; we have no control over them)
     */
    public static final ParseField LOGGER_FIELD = new ParseField("logger");
    public static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp");
    public static final ParseField LEVEL_FIELD = new ParseField("level");
    public static final ParseField PID_FIELD = new ParseField("pid");
    public static final ParseField THREAD_FIELD = new ParseField("thread");
    public static final ParseField MESSAGE_FIELD = new ParseField("message");
    public static final ParseField CLASS_FIELD = new ParseField("class");
    public static final ParseField METHOD_FIELD = new ParseField("method");
    public static final ParseField FILE_FIELD = new ParseField("file");
    public static final ParseField LINE_FIELD = new ParseField("line");

    public static final ObjectParser<CppLogMessage, Void> PARSER = new ObjectParser<>(
            LOGGER_FIELD.getPreferredName(), () -> new CppLogMessage(Instant.now()));

    static {
        PARSER.declareString(CppLogMessage::setLogger, LOGGER_FIELD);
        PARSER.declareField(CppLogMessage::setTimestamp, p -> Instant.ofEpochMilli(p.longValue()), TIMESTAMP_FIELD, ValueType.LONG);
        PARSER.declareString(CppLogMessage::setLevel, LEVEL_FIELD);
        PARSER.declareLong(CppLogMessage::setPid, PID_FIELD);
        PARSER.declareString(CppLogMessage::setThread, THREAD_FIELD);
        PARSER.declareString(CppLogMessage::setMessage, MESSAGE_FIELD);
        PARSER.declareString(CppLogMessage::setClazz, CLASS_FIELD);
        PARSER.declareString(CppLogMessage::setMethod, METHOD_FIELD);
        PARSER.declareString(CppLogMessage::setFile, FILE_FIELD);
        PARSER.declareLong(CppLogMessage::setLine, LINE_FIELD);
    }

    /**
     * Elasticsearch type
     */
    public static final ParseField TYPE = new ParseField("cpp_log_message");

    private String logger = "";
    private Instant timestamp;
    private String level = "";
    private long pid = 0;
    private String thread = "";
    private String message = "";
    private String clazz = "";
    private String method = "";
    private String file = "";
    private long line = 0;

    public CppLogMessage(Instant timestamp) {
        this.timestamp = Instant.ofEpochMilli(timestamp.toEpochMilli());
    }

    public CppLogMessage(StreamInput in) throws IOException {
        logger = in.readString();
        timestamp = in.readInstant();

        level = in.readString();
        pid = in.readVLong();
        thread = in.readString();
        message = in.readString();
        clazz = in.readString();
        method = in.readString();
        file = in.readString();
        line = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(logger);
        out.writeInstant(timestamp);
        out.writeString(level);
        out.writeVLong(pid);
        out.writeString(thread);
        out.writeString(message);
        out.writeString(clazz);
        out.writeString(method);
        out.writeString(file);
        out.writeVLong(line);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LOGGER_FIELD.getPreferredName(), logger);
        builder.field(TIMESTAMP_FIELD.getPreferredName(), timestamp.toEpochMilli());
        builder.field(LEVEL_FIELD.getPreferredName(), level);
        builder.field(PID_FIELD.getPreferredName(), pid);
        builder.field(THREAD_FIELD.getPreferredName(), thread);
        builder.field(MESSAGE_FIELD.getPreferredName(), message);
        builder.field(CLASS_FIELD.getPreferredName(), clazz);
        builder.field(METHOD_FIELD.getPreferredName(), method);
        builder.field(FILE_FIELD.getPreferredName(), file);
        builder.field(LINE_FIELD.getPreferredName(), line);
        builder.endObject();
        return builder;
    }

    public String getLogger() {
        return logger;
    }

    public void setLogger(String logger) {
        this.logger = logger;
    }

    public Instant getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(Instant d) {
        this.timestamp = Instant.ofEpochMilli(d.toEpochMilli());
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
    }

    public String getThread() {
        return thread;
    }

    public void setThread(String thread) {
        this.thread = thread;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * This is unreliable for some C++ compilers - probably best not to display it prominently
     */
    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    /**
     * This is unreliable for some C++ compilers - probably best not to display it prominently
     */
    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public long getLine() {
        return line;
    }

    public void setLine(long line) {
        this.line = line;
    }

    /**
     * Definition of similar message in order to summarize them.
     *
     * Note: Assuming line and file are already unique, paranoia: check that
     * line logging is enabled.
     *
     * @param other
     *            message to compare with
     * @return true if messages are similar
     */
    public boolean isSimilarTo(CppLogMessage other) {
        return other != null && line > 0 && line == other.line && file.equals(other.file) && level.equals(other.level);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logger, timestamp, level, pid, thread, message, clazz, method, file, line);
    }

    /**
     * Compare all the fields.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof CppLogMessage)) {
            return false;
        }

        CppLogMessage that = (CppLogMessage)other;

        return Objects.equals(this.logger, that.logger) && Objects.equals(this.timestamp, that.timestamp)
                && Objects.equals(this.level, that.level) && this.pid == that.pid
                && Objects.equals(this.thread, that.thread) && Objects.equals(this.message, that.message)
                && Objects.equals(this.clazz, that.clazz) && Objects.equals(this.method, that.method)
                && Objects.equals(this.file, that.file) && this.line == that.line;
    }
}
