/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.logfile;

import org.elasticsearch.common.logging.ESLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
*
*/
public class CapturingLogger extends ESLogger {

    private Level level;

    public final List<Msg> error = new ArrayList<>();
    public final List<Msg> warn = new ArrayList<>();
    public final List<Msg> info = new ArrayList<>();
    public final List<Msg> debug = new ArrayList<>();
    public final List<Msg> trace = new ArrayList<>();

    public CapturingLogger(Level level) {
        super(null, null);
        this.level = level;
    }

    @Override
    public void trace(String msg, Throwable cause, Object... params) {
        if (isTraceEnabled()) {
            add(trace, format(msg, params), cause);
        }
    }

    @Override
    public void debug(String msg, Throwable cause, Object... params) {
        if (isDebugEnabled()) {
            add(debug, format(msg, params), cause);
        }
    }

    @Override
    public void info(String msg, Throwable cause, Object... params) {
        if (isInfoEnabled()) {
            add(info, format(msg, params), cause);
        }
    }

    @Override
    public void warn(String msg, Throwable cause, Object... params) {
        if (isWarnEnabled()) {
            add(warn, format(msg, params), cause);
        }
    }

    @Override
    public void error(String msg, Throwable cause, Object... params) {
        if (isErrorEnabled()) {
            add(error, format(msg, params), cause);
        }
    }

    @Override
    public String getName() {
        return "capturing";
    }

    @Override
    public void setLevel(String level) {
        this.level = Level.resolve(level);
    }

    @Override
    public String getLevel() {
        return level.name().toLowerCase(Locale.ROOT);
    }

    public Level level() {
        return level;
    }

    @Override
    public boolean isTraceEnabled() {
        return level.enabled(Level.TRACE);
    }

    @Override
    public boolean isDebugEnabled() {
        return level.enabled(Level.DEBUG);
    }

    @Override
    public boolean isInfoEnabled() {
        return level.enabled(Level.INFO);
    }

    @Override
    public boolean isWarnEnabled() {
        return level.enabled(Level.WARN);
    }

    @Override
    public boolean isErrorEnabled() {
        return level.enabled(Level.ERROR);
    }

    public List<Msg> output(Level level) {
        switch (level) {
            case ERROR: return error;
            case WARN:  return warn;
            case INFO:  return info;
            case DEBUG: return debug;
            case TRACE: return trace;
            default:
                return null; // can never happen
        }
    }

    private static void add(List<Msg> list, String text, Throwable t) {
        list.add(new Msg(text, t));
    }

    public boolean isEmpty() {
        return error.isEmpty() && warn.isEmpty() && info.isEmpty() && debug.isEmpty() && trace.isEmpty();
    }

    public static class Msg {
        public String text;
        public Throwable t;

        public Msg(String text, Throwable t) {
            this.text = text;
            this.t = t;
        }
    }

    public enum Level {
        ERROR(0), WARN(1), INFO(2), DEBUG(3), TRACE(4);

        private final int value;

        private Level(int value) {
            this.value = value;
        }

        public boolean enabled(Level other) {
            return value >= other.value;
        }

        private static Level resolve(String level) {
            return Level.valueOf(level.toUpperCase(Locale.ROOT));
        }
    }
}
