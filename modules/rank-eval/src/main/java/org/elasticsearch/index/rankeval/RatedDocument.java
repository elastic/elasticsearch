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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;

/**
 * A document ID and its rating for the query QA use case.
 * */
public class RatedDocument implements Writeable {

    // TODO augment with index name and type name
    private final String docId;
    private final int rating;

    public RatedDocument(String docId, int rating) {
        this.docId = docId;
        this.rating = rating;
    }

    public RatedDocument(StreamInput in) throws IOException {
        this.docId = in.readString();
        this.rating = in.readVInt();
    }

    public String getDocID() {
        return docId;
    }

    public int getRating() {
        return rating;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(docId);
        out.writeVInt(rating);
    }

    public static RatedDocument fromXContent(XContentParser parser) throws IOException {
        String id = null;
        int rating = Integer.MIN_VALUE;
        Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken().equals(Token.FIELD_NAME)) {
                if (id != null) {
                    throw new ParsingException(parser.getTokenLocation(), "only one document id allowed, found [{}] but already got [{}]",
                            parser.currentName(), id);
                }
                id = parser.currentName();
            } else if (parser.currentToken().equals(Token.VALUE_NUMBER)) {
                rating = parser.intValue();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [{}] while parsing rated document",
                        token);
            }
        }
        if (id ==  null) {
            throw new ParsingException(parser.getTokenLocation(), "didn't find document id");
        }
        return new RatedDocument(id, rating);
    }
}
