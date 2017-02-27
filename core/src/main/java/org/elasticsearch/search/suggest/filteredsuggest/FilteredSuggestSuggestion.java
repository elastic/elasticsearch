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
package org.elasticsearch.search.suggest.filteredsuggest;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;

public class FilteredSuggestSuggestion extends Suggest.Suggestion<FilteredSuggestSuggestion.Entry> {

    public static final int TYPE = 5;

    public FilteredSuggestSuggestion() {
    }

    public FilteredSuggestSuggestion(String name, int size) {
        super(name, size);
    }

    @Override
    public int getWriteableType() {
        return TYPE;
    }

    @Override
    protected Entry newEntry() {
        return new Entry();
    }

    public static class Entry extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry<FilteredSuggestSuggestion.Entry.Option> {
        public Entry(Text text, int offset, int length) {
            super(text, offset, length);
        }

        protected Entry() {
            super();
        }

        @Override
        protected Option newOption() {
            return new Option();
        }

        public static class Option extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option {
            private ScoreDoc doc;

            public Option(Text text, float score, BytesReference payload) {
                super(text, score);
            }

            protected Option() {
                super();
            }

            public Option(int docID, Text text, float score) {
                super(text, score);
                this.doc = new ScoreDoc(docID, score);
            }

            @Override
            public void setScore(float score) {
                super.setScore(score);
            }

            @Override
            protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                super.innerToXContent(builder, params);
                return builder;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                super.readFrom(in);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }
    }

}
