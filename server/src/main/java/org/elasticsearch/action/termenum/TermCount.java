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

package org.elasticsearch.action.termenum;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TermCount implements Writeable, ToXContentFragment {

    public static final String TERM_FIELD = "term";
    public static final String DOC_COUNT_FIELD = "doc_count";

    static final ConstructingObjectParser<TermCount, Void> PARSER = new ConstructingObjectParser<>(
        "term_count",
        true,
        a -> { return new TermCount((String) a[0], (int) a[1]); }
    );
    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField(TERM_FIELD));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(DOC_COUNT_FIELD));
    }

    private String term;

    private int docCount;

    public TermCount(StreamInput in) throws IOException {
        term = in.readString();
        docCount = in.readInt();
    }

    public TermCount(String term, int count) {
        this.term = term;
        this.docCount = count;
    }

    public String getTerm() {
        return this.term;
    }

    public int getDocCount() {
        return this.docCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(term);
        out.writeInt(docCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TERM_FIELD, getTerm());
        builder.field(DOC_COUNT_FIELD, getDocCount());
        return builder;
    }

    public static TermCount fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TermCount other = (TermCount) o;
        return Objects.equals(getTerm(), other.getTerm()) && Objects.equals(getDocCount(), other.getDocCount());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTerm(), getDocCount());
    }

    void addToDocCount(int extra) {
        docCount += extra;
    }

    void setTerm(String term) {
        this.term = term;
    }

    void setDocCount(int docCount) {
        this.docCount = docCount;
    }

}
