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

package org.elasticsearch.common.lucene.all;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.io.FastCharArrayWriter;
import org.elasticsearch.common.io.FastStringReader;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

/**
 *
 */
public class AllEntries extends Reader {

    public static class Entry {
        private final String name;
        private final FastStringReader reader;
        private final int startOffset;
        private final float boost;

        public Entry(String name, FastStringReader reader, int startOffset, float boost) {
            this.name = name;
            this.reader = reader;
            this.startOffset = startOffset;
            this.boost = boost;
        }

        public int startOffset() {
            return startOffset;
        }

        public String name() {
            return this.name;
        }

        public float boost() {
            return this.boost;
        }

        public FastStringReader reader() {
            return this.reader;
        }
    }

    private final List<Entry> entries = Lists.newArrayList();

    private Entry current;

    private Iterator<Entry> it;

    private boolean itsSeparatorTime = false;

    private boolean customBoost = false;

    public void addText(String name, String text, float boost) {
        if (boost != 1.0f) {
            customBoost = true;
        }
        final int lastStartOffset;
        if (entries.isEmpty()) {
            lastStartOffset = -1;
        } else {
            final Entry last = entries.get(entries.size() - 1);
            lastStartOffset = last.startOffset() + last.reader().length();
        }
        final int startOffset = lastStartOffset + 1; // +1 because we insert a space between tokens
        Entry entry = new Entry(name, new FastStringReader(text), startOffset, boost);
        entries.add(entry);
    }

    public boolean customBoost() {
        return customBoost;
    }

    public void clear() {
        this.entries.clear();
        this.current = null;
        this.it = null;
        itsSeparatorTime = false;
    }

    @Override
    public void reset() {
        try {
            for (Entry entry : entries) {
                entry.reader().reset();
            }
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("should not happen");
        }
        it = entries.iterator();
        if (it.hasNext()) {
            current = it.next();
            itsSeparatorTime = true;
        }
    }


    public String buildText() {
        reset();
        FastCharArrayWriter writer = new FastCharArrayWriter();
        for (Entry entry : entries) {
            writer.append(entry.reader());
            writer.append(' ');
        }
        reset();
        return writer.toString();
    }

    public List<Entry> entries() {
        return this.entries;
    }

    public Set<String> fields() {
        Set<String> fields = newHashSet();
        for (Entry entry : entries) {
            fields.add(entry.name());
        }
        return fields;
    }

    // compute the boost for a token with the given startOffset
    public float boost(int startOffset) {
        if (!entries.isEmpty()) {
            int lo = 0, hi = entries.size() - 1;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                final int midOffset = entries.get(mid).startOffset();
                if (startOffset < midOffset) {
                    hi = mid - 1;
                } else {
                    lo = mid + 1;
                }
            }
            final int index = Math.max(0, hi); // protection against broken token streams
            assert entries.get(index).startOffset() <= startOffset;
            assert index == entries.size() - 1 || entries.get(index + 1).startOffset() > startOffset;
            return entries.get(index).boost();
        }
        return 1.0f;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (current == null) {
            return -1;
        }
        if (customBoost) {
            int result = current.reader().read(cbuf, off, len);
            if (result == -1) {
                if (itsSeparatorTime) {
                    itsSeparatorTime = false;
                    cbuf[off] = ' ';
                    return 1;
                }
                itsSeparatorTime = true;
                // close(); No need to close, we work on in mem readers
                if (it.hasNext()) {
                    current = it.next();
                } else {
                    current = null;
                }
                return read(cbuf, off, len);
            }
            return result;
        } else {
            int read = 0;
            while (len > 0) {
                int result = current.reader().read(cbuf, off, len);
                if (result == -1) {
                    if (it.hasNext()) {
                        current = it.next();
                    } else {
                        current = null;
                        if (read == 0) {
                            return -1;
                        }
                        return read;
                    }
                    cbuf[off++] = ' ';
                    read++;
                    len--;
                } else {
                    read += result;
                    off += result;
                    len -= result;
                }
            }
            return read;
        }
    }

    @Override
    public void close() {
        if (current != null) {
            // no need to close, these are readers on strings
            current = null;
        }
    }


    @Override
    public boolean ready() throws IOException {
        return (current != null) && current.reader().ready();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Entry entry : entries) {
            sb.append(entry.name()).append(',');
        }
        return sb.toString();
    }
}
