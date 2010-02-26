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

package org.elasticsearch.action.mlt;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.Actions;
import org.elasticsearch.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class MoreLikeThisRequest implements ActionRequest {

    private String index;

    private String type;

    private String id;

    private String[] fields;

    private float percentTermsToMatch = -1;
    private int minTermFrequency = -1;
    private int maxQueryTerms = -1;
    private String[] stopWords = null;
    private int minDocFreq = -1;
    private int maxDocFreq = -1;
    private int minWordLen = -1;
    private int maxWordLen = -1;
    private Boolean boostTerms = null;
    private float boostTermsFactor = -1;

    private boolean threadedListener = false;

    public MoreLikeThisRequest() {
    }

    public MoreLikeThisRequest(String index) {
        this.index = index;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public MoreLikeThisRequest type(String type) {
        this.type = type;
        return this;
    }

    public String id() {
        return id;
    }

    public MoreLikeThisRequest id(String id) {
        this.id = id;
        return this;
    }

    public String[] fields() {
        return this.fields;
    }

    public MoreLikeThisRequest percentTermsToMatch(float percentTermsToMatch) {
        this.percentTermsToMatch = percentTermsToMatch;
        return this;
    }

    public float percentTermsToMatch() {
        return this.percentTermsToMatch;
    }

    public MoreLikeThisRequest minTermFrequency(int minTermFrequency) {
        this.minTermFrequency = minTermFrequency;
        return this;
    }

    public int minTermFrequency() {
        return this.minTermFrequency;
    }

    public MoreLikeThisRequest maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    public int maxQueryTerms() {
        return this.maxQueryTerms;
    }

    public MoreLikeThisRequest stopWords(String... stopWords) {
        this.stopWords = stopWords;
        return this;
    }

    public String[] stopWords() {
        return this.stopWords;
    }

    public MoreLikeThisRequest minDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    public int minDocFreq() {
        return this.minDocFreq;
    }

    public MoreLikeThisRequest maxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    public int maxDocFreq() {
        return this.maxDocFreq;
    }

    public MoreLikeThisRequest minWordLen(int minWordLen) {
        this.minWordLen = minWordLen;
        return this;
    }

    public int minWordLen() {
        return this.minWordLen;
    }

    public MoreLikeThisRequest maxWordLen(int maxWordLen) {
        this.maxWordLen = maxWordLen;
        return this;
    }

    public int maxWordLen() {
        return this.maxWordLen;
    }

    public MoreLikeThisRequest boostTerms(boolean boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    public Boolean boostTerms() {
        return this.boostTerms;
    }

    public MoreLikeThisRequest boostTermsFactor(float boostTermsFactor) {
        this.boostTermsFactor = boostTermsFactor;
        return this;
    }

    public float boostTermsFactor() {
        return this.boostTermsFactor;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = Actions.addValidationError("index is missing", validationException);
        }
        if (type == null) {
            validationException = Actions.addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = Actions.addValidationError("id is missing", validationException);
        }
        return validationException;
    }

    @Override public boolean listenerThreaded() {
        return threadedListener;
    }

    @Override public ActionRequest listenerThreaded(boolean listenerThreaded) {
        this.threadedListener = listenerThreaded;
        return this;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        index = in.readUTF();
        type = in.readUTF();
        id = in.readUTF();
        // no need to pass threading over the network, they are always false when coming throw a thread pool
        int size = in.readInt();
        if (size == 0) {
            fields = Strings.EMPTY_ARRAY;
        } else {
            fields = new String[in.readInt()];
            for (int i = 0; i < size; i++) {
                fields[i] = in.readUTF();
            }
        }

        percentTermsToMatch = in.readFloat();
        minTermFrequency = in.readInt();
        maxQueryTerms = in.readInt();
        size = in.readInt();
        if (size > 0) {
            stopWords = new String[size];
            for (int i = 0; i < size; i++) {
                stopWords[i] = in.readUTF();
            }
        }
        minDocFreq = in.readInt();
        maxDocFreq = in.readInt();
        minWordLen = in.readInt();
        maxWordLen = in.readInt();
        if (in.readBoolean()) {
            boostTerms = in.readBoolean();
        }
        boostTermsFactor = in.readFloat();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(index);
        out.writeUTF(type);
        out.writeUTF(id);
        if (fields == null) {
            out.writeInt(0);
        } else {
            out.writeInt(fields.length);
            for (String field : fields) {
                out.writeUTF(field);
            }
        }

        out.writeFloat(percentTermsToMatch);
        out.writeInt(minTermFrequency);
        out.writeInt(maxQueryTerms);
        if (stopWords == null) {
            out.writeInt(0);
        } else {
            out.writeInt(stopWords.length);
            for (String stopWord : stopWords) {
                out.writeUTF(stopWord);
            }
        }
        out.writeInt(minDocFreq);
        out.writeInt(maxDocFreq);
        out.writeInt(minWordLen);
        out.writeInt(maxWordLen);
        if (boostTerms == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeBoolean(boostTerms);
        }
        out.writeFloat(boostTermsFactor);
    }
}
