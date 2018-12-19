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
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.StringBuilderFormattable;
import org.apache.logging.log4j.util.Strings;

import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class MarkerLogger extends Logger {


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
     * The constructor.
     * //TODO consider what happens when UNKOWN_NODE_ID are not in use anymore
     */
    protected MarkerLogger(Logger logger, AtomicReference<NodeIdListener> nodeIdListener) {
        super(logger.getContext(), logger.getName(), logger.getMessageFactory());
        String prefix = getPrefix(nodeIdListener);

        final Marker actualMarker;
        // markers is not thread-safe, so we synchronize access
        synchronized (markers) {
            final Marker maybeMarker = markers.get(prefix);
            if (maybeMarker == null) {
                if (nodeIdListener.get() != null && Strings.isNotEmpty(nodeIdListener.get().getNodeId().get())) {
                    actualMarker = new MarkerManager.Log4jMarker(prefix);
                } else {
                    actualMarker = new AtomicRefMarker(nodeIdListener);
                }

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

    private String getPrefix(AtomicReference<NodeIdListener> nodeIdListenerRef) {
        NodeIdListener nodeIdListener = nodeIdListenerRef.get();
        if (nodeIdListener != null) {
            AtomicReference<String> nodeId = nodeIdListener.getNodeId();
            return nodeId.get();
        }
        return NodeIdListener.UNKOWN_NODE_ID;
    }


    @Override
    public void logMessage(final String fqcn, final Level level, final Marker marker, final Message message, final Throwable t) {
        super.logMessage(fqcn, level, this.marker, message, t);
    }

    static class AtomicRefMarker implements Marker, StringBuilderFormattable {

        private AtomicReference<NodeIdListener> nodeId;

        /**
         * Constructs a new Marker.
         */
        AtomicRefMarker(AtomicReference<NodeIdListener> nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public String getName() {
            if (nodeId.get() != null && Strings.isNotEmpty(nodeId.get().getNodeId().get())) {
                return nodeId.get().getNodeId().get();
            }
            return NodeIdListener.UNKOWN_NODE_ID;
        }

        @Override
        public void formatTo(StringBuilder buffer) {
            buffer.append(getName());
        }
        //just using the marker for its name in logs since other methos are unimplemented

        @Override
        public Marker addParents(Marker... markers) {
            return null;
        }

        @Override
        public Marker[] getParents() {
            return new Marker[0];
        }

        @Override
        public boolean hasParents() {
            return false;
        }

        @Override
        public boolean isInstanceOf(Marker m) {
            return false;
        }

        @Override
        public boolean isInstanceOf(String name) {
            return false;
        }

        @Override
        public boolean remove(Marker marker) {
            return false;
        }

        @Override
        public Marker setParents(Marker... markers) {
            return null;
        }


    }
}
