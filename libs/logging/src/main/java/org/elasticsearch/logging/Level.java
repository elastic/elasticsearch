/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Levels used for identifying the severity of an event.
 *
 * Level has a name and a severity. A severity is used to log events which are less verbose (higher serverit and importance).
 * Previously created instances can be obtained by name.
 */
public final class Level {
    /**
     * No events will be logged.
     */
    public static final Level OFF = new Level("OFF", StandardLevels.OFF);
    /**
     * A fatal error that will prevent the application from continuing.
     */
    public static final Level FATAL = new Level("FATAL", StandardLevels.FATAL);
    /**
     * An error in the application, possibly recoverable.
     */
    public static final Level ERROR = new Level("ERROR", StandardLevels.ERROR);
    /**
     * An event that might possible lead to an error.
     */
    public static final Level WARN = new Level("WARN", StandardLevels.WARN);
    /**
     * An event for informational purposes.
     */
    public static final Level INFO = new Level("INFO", StandardLevels.INFO);
    /**
     * A general debugging event.
     */
    public static final Level DEBUG = new Level("DEBUG", StandardLevels.DEBUG);
    /**
     * A fine-grained debug message, typically capturing the flow through the application.
     */
    public static final Level TRACE = new Level("TRACE", StandardLevels.TRACE);
    /**
     * All events should be logged.
     */
    public static final Level ALL = new Level("ALL", StandardLevels.ALL);

    private static final ConcurrentMap<String, Level> LEVELS = new ConcurrentHashMap<>();

    static {
        LEVELS.put(OFF.name, OFF);
        LEVELS.put(FATAL.name, FATAL);
        LEVELS.put(ERROR.name, ERROR);
        LEVELS.put(WARN.name, WARN);
        LEVELS.put(INFO.name, INFO);
        LEVELS.put(DEBUG.name, DEBUG);
        LEVELS.put(TRACE.name, TRACE);
        LEVELS.put(ALL.name, ALL);
    }

    private final String name;
    private final int severity;

    private Level(String name, int severity) {
        this.name = name;
        this.severity = severity;
    }

    /**
     * Retrieves an existing Level or creates on if it didn't previously exist.
     *
     * @param name The name of the level.
     * @param severity The integer value for the Level. If the level was previously created this value is ignored.
     * @return The Level.
     * @throws java.lang.IllegalArgumentException if the name is null or intValue is less than zero.
     */
    public static Level forName(String name, int severity) {
        Objects.requireNonNull(name);

        final String levelName = name.trim().toUpperCase(Locale.ROOT);
        var level = new Level(levelName, severity);
        if (LEVELS.putIfAbsent(levelName, level) != null) {
            return LEVELS.get(levelName);
        }
        return level;
    }

    /**
     * Return the Level associated with the name.
     *
     * @param name The name of the Level to return.
     * @return The Level.
     * @throws java.lang.NullPointerException if the Level name is {@code null}.
     * @throws java.lang.IllegalArgumentException if the Level name is not registered.
     */
    public static Level forName(final String name) {
        Objects.requireNonNull(name);
        final String levelName = name.trim().toUpperCase(Locale.ROOT);
        final Level level = LEVELS.get(levelName);
        if (level != null) {
            return level;
        }
        throw new IllegalArgumentException("Unknown level constant [" + levelName + "].");
    }

    public static Collection<Level> values() {
        return LEVELS.values();
    }

    @Override
    public String toString() {
        return this.name;
    }

    /**
     * Returns the name of this level.
     */
    public String name() {
        return name;
    }

    public int getSeverity() {
        return severity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Level level = (Level) o;
        return severity == level.severity && Objects.equals(name, level.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, severity);
    }

    /**
     * Used for mapping between the API and the implementation
     */
    public static class StandardLevels {

        public static final int OFF = 0;

        public static final int FATAL = 100;

        public static final int ERROR = 200;

        public static final int WARN = 300;

        public static final int INFO = 400;

        public static final int DEBUG = 500;

        public static final int TRACE = 600;

        public static final int ALL = Integer.MAX_VALUE;

        private StandardLevels() {}
    }
}
