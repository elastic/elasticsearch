/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import java.util.WeakHashMap;

/**
 * A logger that prefixes all messages with a fixed prefix specified during construction. The prefix mechanism uses the marker construct, so
 * for the prefixes to appear, the logging layout pattern must include the marker in its pattern.
 */
class PrefixLogger extends ExtendedLoggerWrapper {

    /*
     * We can not use the built-in Marker tracking (MarkerManager) because the MarkerManager holds a permanent reference to the marker;
     * however, we have transient markers from index-level and shard-level components so this would effectively be a memory leak. Since we
     * can not tie into the lifecycle of these components, we have to use a mechanism that enables garbage collection of such markers when
     * they are no longer in use.
     */
    private static final WeakHashMap<String, Marker> markers = new WeakHashMap<>();

    /**
     * Return the size of the cached markers. This size can vary as markers are cached but collected during GC activity when a given prefix
     * is no longer in use.
     *
     * @return the size of the cached markers
     */
    static int markersSize() {
        return markers.size();
    }

    /**
     * The marker for this prefix logger.
     */
    private final Marker marker;

    /**
     * Obtain the prefix for this prefix logger. This can be used to create a logger with the same prefix as this one.
     *
     * @return the prefix
     */
    public String prefix() {
        return marker.getName();
    }

    /**
     * Construct a prefix logger with the specified name and prefix.
     *
     * @param logger the extended logger to wrap
     * @param prefix the prefix for this prefix logger
     */
    PrefixLogger(final Logger logger, final String prefix) {
        super((ExtendedLogger) logger, logger.getName(), null);

        if (prefix == null || prefix.isEmpty()) {
            throw new IllegalArgumentException("if you don't need a prefix then use a regular logger");
        }
        final Marker actualMarker;
        // markers is not thread-safe, so we synchronize access
        synchronized (markers) {
            final Marker maybeMarker = markers.get(prefix);
            if (maybeMarker == null) {
                actualMarker = new MarkerManager.Log4jMarker(prefix);
                /*
                 * We must create a new instance here as otherwise the marker will hold a reference to the key in the weak hash map; as
                 * those references are held strongly, this would give a strong reference back to the key preventing them from ever being
                 * collected. This also guarantees that no other strong reference can be held to the prefix anywhere.
                 */
                // noinspection RedundantStringConstructorCall
                markers.put(new String(prefix), actualMarker);
            } else {
                actualMarker = maybeMarker;
            }
        }
        this.marker = actualMarker;
    }

    @Override
    public void logMessage(final String fqcn, final Level level, final Marker marker, final Message message, final Throwable t) {
        assert marker == null;
        super.logMessage(fqcn, level, this.marker, message, t);
    }

}
