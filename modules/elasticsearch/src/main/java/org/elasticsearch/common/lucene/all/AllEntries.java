/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.common.lucene.all;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.CharSequenceReader;
import org.elasticsearch.common.io.FastCharArrayWriter;
import org.elasticsearch.common.io.FastStringReader;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.collect.Sets.*;

/**
 * @author kimchy (shay.banon)
 */
public class AllEntries extends Reader {

    public static class Entry {
        private final String name;
        private final CharSequenceReader reader;
        private final float boost;

        public Entry(String name, CharSequenceReader reader, float boost) {
            this.name = name;
            this.reader = reader;
            this.boost = boost;
        }

        public String name() {
            return this.name;
        }

        public float boost() {
            return this.boost;
        }

        public CharSequenceReader reader() {
            return this.reader;
        }
    }

    private final List<Entry> entries = Lists.newArrayList();

    private Entry current;

    private Iterator<Entry> it;

    private boolean itsSeparatorTime = false;

    public void addText(String name, String text, float boost) {
        Entry entry = new Entry(name, new FastStringReader(text), boost);
        entries.add(entry);
    }

    public void clear() {
        this.entries.clear();
        this.current = null;
        this.it = null;
        itsSeparatorTime = false;
    }

    public void reset() {
        try {
            for (Entry entry : entries) {
                entry.reader().reset();
            }
        } catch (IOException e) {
            throw new ElasticSearchIllegalStateException("should not happen");
        }
        it = entries.iterator();
        if (it.hasNext()) {
            current = it.next();
            itsSeparatorTime = true;
        }
    }


    public String buildText() {
        reset();
        FastCharArrayWriter writer = FastCharArrayWriter.Cached.cached();
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

    public Entry current() {
        return this.current;
    }

    @Override public int read(char[] cbuf, int off, int len) throws IOException {
        if (current == null) {
            return -1;
        }
        int result = current.reader().read(cbuf, off, len);
        if (result == -1) {
            if (itsSeparatorTime) {
                itsSeparatorTime = false;
                cbuf[off] = ' ';
                return 1;
            }
            itsSeparatorTime = true;
            advance();
            return read(cbuf, off, len);
        }
        return result;
    }

    @Override public void close() {
        if (current != null) {
            try {
                current.reader().close();
            } catch (IOException e) {
                // can't happen...
            } finally {
                current = null;
            }
        }
    }


    @Override public boolean ready() throws IOException {
        return (current != null) && current.reader().ready();
    }

    /**
     * Closes the current reader and opens the next one, if any.
     */
    private void advance() {
        close();
        if (it.hasNext()) {
            current = it.next();
        }
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Entry entry : entries) {
            sb.append(entry.name()).append(',');
        }
        return sb.toString();
    }
}
