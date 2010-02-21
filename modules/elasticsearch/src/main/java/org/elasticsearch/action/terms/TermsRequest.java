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

package org.elasticsearch.action.terms;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.util.Required;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.action.Actions.*;

/**
 * Terms request represent a request to get terms in one or more indices of specific fields and their
 * document frequencies (in how many document each term exists).
 *
 * <p>This is very handy to implement things like tag clouds and auto complete (using {@link #prefix(String)} or
 * {@link #regexp(String)}).
 *
 * @author kimchy (Shay Banon)
 */
public class TermsRequest extends BroadcastOperationRequest {

    /**
     * The type of sorting for terms.
     */
    public static enum SortType {
        /**
         * Sort based on the term (lex).
         */
        TERM((byte) 0),
        /**
         * Sort based on the term document frequency.
         */
        FREQ((byte) 1);

        private byte value;

        SortType(byte value) {
            this.value = value;
        }

        /**
         * The unique byte value of the sort type.
         */
        public byte value() {
            return value;
        }

        /**
         * Parses the sort type from its {@link #value()}.
         */
        public static SortType fromValue(byte value) {
            switch (value) {
                case 0:
                    return TERM;
                case 1:
                    return FREQ;
                default:
                    throw new ElasticSearchIllegalArgumentException("No value for [" + value + "]");
            }
        }

        /**
         * Parses the sort type from a string. Can either be "term" or "freq". If <tt>null</tt>
         * is passed, will return the defaultSort provided.
         *
         * @param value       The string value to parse. Can be either "term" or "freq"
         * @param defaultSort The sort type to return in case value is <tt>null</tt>
         * @return The sort type parsed
         */
        public static SortType fromString(String value, SortType defaultSort) {
            if (value == null) {
                return defaultSort;
            }
            if (value.equals("term")) {
                return TERM;
            } else if (value.equals("freq")) {
                return FREQ;
            } else {
                throw new ElasticSearchIllegalArgumentException("Illegal sort type [" + value + "], must be one of [term,freq]");
            }
        }
    }

    private String[] fields;

    private String from;

    private boolean fromInclusive = true;

    private String to;

    private boolean toInclusive = false;

    private String prefix;

    private String regexp;

    private int minFreq = 1;

    private int maxFreq = Integer.MAX_VALUE;

    private int size = 10;

    private boolean convert = true;

    private SortType sortType = SortType.TERM;

    private boolean exact = false;

    TermsRequest() {
    }

    /**
     * Constructs a new terms requests with the provided indices. Don't pass anything for it to run
     * over all the indices. Note, the {@link #fields(String...)} is required.
     */
    public TermsRequest(String... indices) {
        super(indices, null);
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (fields == null || fields.length == 0) {
            validationException = addValidationError("fields is missing", validationException);
        }
        return validationException;
    }

    /**
     * The fields within each document which terms will be iterated over and returned with the
     * document frequencies.
     */
    public String[] fields() {
        return this.fields;
    }

    /**
     * The fields within each document which terms will be iterated over and returned with the
     * document frequencies.
     */
    @Required public TermsRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * The lower bound (lex) term from which the iteration will start.  Defaults to start from the
     * first.
     */
    public String from() {
        return from;
    }

    /**
     * The lower bound (lex) term from which the iteration will start.  Defaults to start from the
     * first.
     */
    public TermsRequest from(String from) {
        this.from = from;
        return this;
    }

    /**
     * Should the first from (if set using {@link #from(String)} be inclusive or not. Defaults
     * to <tt>false</tt> (not inclusive / exclusive).
     */
    public boolean fromInclusive() {
        return fromInclusive;
    }

    /**
     * Should the first from (if set using {@link #from(String)} be inclusive or not. Defaults
     * to <tt>false</tt> (not inclusive / exclusive).
     */
    public TermsRequest fromInclusive(boolean fromInclusive) {
        this.fromInclusive = fromInclusive;
        return this;
    }

    /**
     * The upper bound (lex) term to which the iteration will end. Defaults to unbound (<tt>null</tt>).
     */
    public String to() {
        return to;
    }

    /**
     * The upper bound (lex) term to which the iteration will end. Defaults to unbound (<tt>null</tt>).
     */
    public TermsRequest to(String to) {
        this.to = to;
        return this;
    }

    /**
     * Should the last to (if set using {@link #to(String)} be inclusive or not. Defaults to
     * <tt>true</tt>.
     */
    public boolean toInclusive() {
        return toInclusive;
    }

    /**
     * Should the last to (if set using {@link #to(String)} be inclusive or not. Defaults to
     * <tt>true</tt>.
     */
    public TermsRequest toInclusive(boolean toInclusive) {
        this.toInclusive = toInclusive;
        return this;
    }

    /**
     * An optional prefix from which the terms iteration will start (in lex order).
     */
    public String prefix() {
        return prefix;
    }

    /**
     * An optional prefix from which the terms iteration will start (in lex order).
     */
    public TermsRequest prefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * An optional regular expression to filter out terms (only the ones that match the regexp
     * will return).
     */
    public String regexp() {
        return regexp;
    }

    /**
     * An optional regular expression to filter out terms (only the ones that match the regexp
     * will return).
     */
    public void regexp(String regexp) {
        this.regexp = regexp;
    }

    /**
     * An optional minimum document frequency to filter out terms.
     */
    public int minFreq() {
        return minFreq;
    }

    /**
     * An optional minimum document frequency to filter out terms.
     */
    public TermsRequest minFreq(int minFreq) {
        this.minFreq = minFreq;
        return this;
    }

    /**
     * An optional maximum document frequency to filter out terms.
     */
    public int maxFreq() {
        return maxFreq;
    }

    /**
     * An optional maximum document frequency to filter out terms.
     */
    public TermsRequest maxFreq(int maxFreq) {
        this.maxFreq = maxFreq;
        return this;
    }

    /**
     * The number of term / doc freq pairs to return per field. Defaults to <tt>10</tt>.
     */
    public int size() {
        return size;
    }

    /**
     * The number of term / doc freq pairs to return per field. Defaults to <tt>10</tt>.
     */
    public TermsRequest size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Should an attempt be made to convert the {@link #to(String)} and {@link #from(String)}.
     * Defaults to <tt>true</tt>.
     */
    public boolean convert() {
        return convert;
    }

    /**
     * Should an attempt be made to convert the {@link #to(String)} and {@link #from(String)}.
     * Defaults to <tt>true</tt>.
     */
    public TermsRequest convert(boolean convert) {
        this.convert = convert;
        return this;
    }

    /**
     * The type of sorting for term / doc freq. Can either sort on term (lex) or doc frequency. Defaults to
     * {@link TermsRequest.SortType#TERM}.
     */
    public SortType sortType() {
        return sortType;
    }

    /**
     * The type of sorting for term / doc freq. Can either sort on term (lex) or doc frequency. Defaults to
     * {@link TermsRequest.SortType#TERM}.
     */
    public TermsRequest sortType(SortType sortType) {
        this.sortType = sortType;
        return this;
    }

    /**
     * Should the doc frequencies be exact frequencies. Exact frequencies takes into account deletes that
     * have not been merged and cleaned (optimized). Note, when this is set to <tt>true</tt> this operation
     * might be an expensive operation. Defaults to <tt>false</tt>.
     */
    public boolean exact() {
        return exact;
    }

    /**
     * Should the doc frequencies be exact frequencies. Exact frequencies takes into account deletes that
     * have not been merged and cleaned (optimized). Note, when this is set to <tt>true</tt> this operation
     * might be an expensive operation. Defaults to <tt>false</tt>.
     */
    public TermsRequest exact(boolean exact) {
        this.exact = exact;
        return this;
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(fields.length);
        for (String field : fields) {
            out.writeUTF(field);
        }
        if (from == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(from);
        }
        if (to == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(to);
        }
        out.writeBoolean(fromInclusive);
        out.writeBoolean(toInclusive);
        if (prefix == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(prefix);
        }
        if (regexp == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(regexp);
        }
        out.writeInt(size);
        out.writeBoolean(convert);
        out.writeByte(sortType.value());
        out.writeInt(minFreq);
        out.writeInt(maxFreq);
        out.writeBoolean(exact);
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        fields = new String[in.readInt()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = in.readUTF();
        }
        if (in.readBoolean()) {
            from = in.readUTF();
        }
        if (in.readBoolean()) {
            to = in.readUTF();
        }
        fromInclusive = in.readBoolean();
        toInclusive = in.readBoolean();
        if (in.readBoolean()) {
            prefix = in.readUTF();
        }
        if (in.readBoolean()) {
            regexp = in.readUTF();
        }
        size = in.readInt();
        convert = in.readBoolean();
        sortType = TermsRequest.SortType.fromValue(in.readByte());
        minFreq = in.readInt();
        maxFreq = in.readInt();
        exact = in.readBoolean();
    }
}
