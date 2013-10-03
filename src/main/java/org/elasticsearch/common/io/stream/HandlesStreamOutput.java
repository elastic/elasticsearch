/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.io.stream;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.elasticsearch.common.text.Text;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class HandlesStreamOutput extends AdapterStreamOutput {

    private static final int DEFAULT_IDENTITY_THRESHOLD = 50;

    // a threshold above which strings will use identity check
    private final int identityThreshold;

    private ObjectIntOpenHashMap<String> handles = new ObjectIntOpenHashMap<String>();
    private HandleTable identityHandles = new HandleTable(10, (float) 3.00);
    private ObjectIntOpenHashMap<Text> handlesText = new ObjectIntOpenHashMap<Text>();

    public HandlesStreamOutput(StreamOutput out) {
        this(out, DEFAULT_IDENTITY_THRESHOLD);
    }

    public HandlesStreamOutput(StreamOutput out, int identityThreshold) {
        super(out);
        this.identityThreshold = identityThreshold;
    }

    @Override
    public void writeString(String s) throws IOException {
        if (s.length() < identityThreshold) {
            if (!handles.containsKey(s)) {
                int handle = handles.size();
                handles.put(s, handle);
                out.writeByte((byte) 0);
                out.writeVInt(handle);
                out.writeString(s);
            } else {
                out.writeByte((byte) 1);
                out.writeVInt(handles.lget());
            }
        } else {
            int handle = identityHandles.lookup(s);
            if (handle == -1) {
                handle = identityHandles.assign(s);
                out.writeByte((byte) 2);
                out.writeVInt(handle);
                out.writeString(s);
            } else {
                out.writeByte((byte) 3);
                out.writeVInt(handle);
            }
        }
    }

    @Override
    public void writeSharedText(Text text) throws IOException {
        int length;
        if (text.hasBytes()) {
            length = text.bytes().length();
        } else {
            length = text.string().length();
        }
        if (length < identityThreshold) {
            if (!handlesText.containsKey(text)) {
                int handle = handlesText.size();
                handlesText.put(text, handle);
                out.writeByte((byte) 0);
                out.writeVInt(handle);
                out.writeText(text);
            } else {
                out.writeByte((byte) 1);
                out.writeVInt(handlesText.lget());
            }
        } else {
            out.writeByte((byte) 2);
            out.writeText(text);
        }
    }

    @Override
    public void reset() throws IOException {
        clear();
        if (out != null) {
            out.reset();
        }
    }

    public void clear() {
        handles.clear();
        if (identityHandles.capacity() > 10000) {
            identityHandles = new HandleTable(10, (float) 3.00);
        } else {
            identityHandles.clear();
        }
        handlesText.clear();
    }

    /**
     * Lightweight identity hash table which maps objects to integer handles,
     * assigned in ascending order.
     */
    private static class HandleTable {

        /* number of mappings in table/next available handle */
        private int size;
        /* size threshold determining when to expand hash spine */
        private int threshold;
        /* factor for computing size threshold */
        private final float loadFactor;
        /* maps hash value -> candidate handle value */
        private int[] spine;
        /* maps handle value -> next candidate handle value */
        private int[] next;
        /* maps handle value -> associated object */
        private Object[] objs;

        /**
         * Creates new HandleTable with given capacity and load factor.
         */
        HandleTable(int initialCapacity, float loadFactor) {
            this.loadFactor = loadFactor;
            spine = new int[initialCapacity];
            next = new int[initialCapacity];
            objs = new Object[initialCapacity];
            threshold = (int) (initialCapacity * loadFactor);
            clear();
        }

        public int capacity() {
            return spine.length;
        }

        /**
         * Assigns next available handle to given object, and returns handle
         * value.  Handles are assigned in ascending order starting at 0.
         */
        int assign(Object obj) {
            if (size >= next.length) {
                growEntries();
            }
            if (size >= threshold) {
                growSpine();
            }
            insert(obj, size);
            return size++;
        }

        /**
         * Looks up and returns handle associated with given object, or -1 if
         * no mapping found.
         */
        int lookup(Object obj) {
            if (size == 0) {
                return -1;
            }
            int index = hash(obj) % spine.length;
            for (int i = spine[index]; i >= 0; i = next[i]) {
                if (objs[i] == obj) {
                    return i;
                }
            }
            return -1;
        }

        /**
         * Resets table to its initial (empty) state.
         */
        void clear() {
            Arrays.fill(spine, -1);
            Arrays.fill(objs, 0, size, null);
            size = 0;
        }

        /**
         * Returns the number of mappings currently in table.
         */
        int size() {
            return size;
        }

        /**
         * Inserts mapping object -> handle mapping into table.  Assumes table
         * is large enough to accommodate new mapping.
         */
        private void insert(Object obj, int handle) {
            int index = hash(obj) % spine.length;
            objs[handle] = obj;
            next[handle] = spine[index];
            spine[index] = handle;
        }

        /**
         * Expands the hash "spine" -- equivalent to increasing the number of
         * buckets in a conventional hash table.
         */
        private void growSpine() {
            spine = new int[(spine.length << 1) + 1];
            threshold = (int) (spine.length * loadFactor);
            Arrays.fill(spine, -1);
            for (int i = 0; i < size; i++) {
                insert(objs[i], i);
            }
        }

        /**
         * Increases hash table capacity by lengthening entry arrays.
         */
        private void growEntries() {
            int newLength = (next.length << 1) + 1;
            int[] newNext = new int[newLength];
            System.arraycopy(next, 0, newNext, 0, size);
            next = newNext;

            Object[] newObjs = new Object[newLength];
            System.arraycopy(objs, 0, newObjs, 0, size);
            objs = newObjs;
        }

        /**
         * Returns hash value for given object.
         */
        private int hash(Object obj) {
            return System.identityHashCode(obj) & 0x7FFFFFFF;
        }
    }
}
