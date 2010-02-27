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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.util.Bytes;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.action.Actions.*;
import static org.elasticsearch.search.Scroll.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class SearchRequest implements ActionRequest {

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices;

    private String queryHint;

    private byte[] source;

    private byte[] extraSource;

    private Scroll scroll;

    private int from = -1;

    private int size = -1;

    private String[] types = Strings.EMPTY_ARRAY;

    private TimeValue timeout;

    private boolean listenerThreaded = false;
    private SearchOperationThreading operationThreading = SearchOperationThreading.SINGLE_THREAD;

    SearchRequest() {
    }

    public SearchRequest(String... indices) {
        this.indices = indices;
    }

    public SearchRequest(String index, SearchSourceBuilder source) {
        this(index, source.build());
    }

    public SearchRequest(String index, byte[] source) {
        this(new String[]{index}, source);
    }

    public SearchRequest(String[] indices, SearchSourceBuilder source) {
        this(indices, source.build());
    }

    public SearchRequest(String[] indices, byte[] source) {
        this.indices = indices;
        this.source = source;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = addValidationError("search source is missing", validationException);
        }
        return validationException;
    }

    @Override public boolean listenerThreaded() {
        return listenerThreaded;
    }

    @Override public SearchRequest listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return this;
    }

    public SearchOperationThreading operationThreading() {
        return this.operationThreading;
    }

    public SearchRequest operationThreading(SearchOperationThreading operationThreading) {
        this.operationThreading = operationThreading;
        return this;
    }

    public SearchRequest searchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    /**
     * The source of the search request.
     */
    public SearchRequest source(SearchSourceBuilder sourceBuilder) {
        return source(sourceBuilder.build());
    }

    public SearchRequest source(byte[] source) {
        this.source = source;
        return this;
    }

    /**
     * Allows to provide an additional source that will be used as well.
     */
    public SearchRequest extraSource(SearchSourceBuilder sourceBuilder) {
        return extraSource(sourceBuilder.build());
    }

    /**
     * Allows to provide an additional source that will be used as well.
     */
    public SearchRequest extraSource(byte[] source) {
        this.source = source;
        return this;
    }

    public SearchType searchType() {
        return searchType;
    }

    public String[] indices() {
        return indices;
    }

    public SearchRequest queryHint(String queryHint) {
        this.queryHint = queryHint;
        return this;
    }

    public String queryHint() {
        return queryHint;
    }

    public byte[] source() {
        return source;
    }

    public byte[] extraSource() {
        return this.extraSource;
    }

    public Scroll scroll() {
        return scroll;
    }

    public SearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public int from() {
        return from;
    }

    public SearchRequest from(int from) {
        this.from = from;
        return this;
    }

    public String[] types() {
        return types;
    }

    public SearchRequest types(String... types) {
        this.types = types;
        return this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public SearchRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public int size() {
        return size;
    }

    public SearchRequest size(int size) {
        this.size = size;
        return this;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        operationThreading = SearchOperationThreading.fromId(in.readByte());
        searchType = SearchType.fromId(in.readByte());

        indices = new String[in.readInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readUTF();
        }

        if (in.readBoolean()) {
            queryHint = in.readUTF();
        }

        if (in.readBoolean()) {
            scroll = readScroll(in);
        }
        from = in.readInt();
        size = in.readInt();
        if (in.readBoolean()) {
            timeout = readTimeValue(in);
        }
        int size = in.readInt();
        if (size == 0) {
            source = Bytes.EMPTY_ARRAY;
        } else {
            source = new byte[size];
            in.readFully(source);
        }
        size = in.readInt();
        if (size == 0) {
            extraSource = Bytes.EMPTY_ARRAY;
        } else {
            extraSource = new byte[size];
            in.readFully(extraSource);
        }

        int typesSize = in.readInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeByte(operationThreading.id());
        out.writeByte(searchType.id());

        out.writeInt(indices.length);
        for (String index : indices) {
            out.writeUTF(index);
        }

        if (queryHint == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryHint);
        }

        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
        out.writeInt(from);
        out.writeInt(size);
        if (timeout == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            timeout.writeTo(out);
        }
        if (source == null) {
            out.writeInt(0);
        } else {
            out.writeInt(source.length);
            out.write(source);
        }
        if (extraSource == null) {
            out.writeInt(0);
        } else {
            out.writeInt(extraSource.length);
            out.write(extraSource);
        }
        out.writeInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}
