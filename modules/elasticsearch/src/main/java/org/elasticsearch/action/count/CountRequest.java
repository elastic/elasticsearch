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

package org.elasticsearch.action.count;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.Required;
import org.elasticsearch.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class CountRequest extends BroadcastOperationRequest {

    public static final float DEFAULT_MIN_SCORE = -1f;

    private float minScore = DEFAULT_MIN_SCORE;
    @Required private String querySource;
    private String[] types = Strings.EMPTY_ARRAY;
    @Nullable private String queryParserName;

    CountRequest() {
    }

    public CountRequest(String... indices) {
        super(indices, null);
    }

    @Override public CountRequest operationThreading(BroadcastOperationThreading operationThreading) {
        super.operationThreading(operationThreading);
        return this;
    }

    @Override public CountRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    public CountRequest queryHint(String queryHint) {
        this.queryHint = queryHint;
        return this;
    }

    float minScore() {
        return minScore;
    }

    public CountRequest minScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    String querySource() {
        return querySource;
    }

    @Required public CountRequest querySource(QueryBuilder queryBuilder) {
        return querySource(queryBuilder.build());
    }

    public CountRequest querySource(String querySource) {
        this.querySource = querySource;
        return this;
    }

    String queryParserName() {
        return queryParserName;
    }

    public CountRequest queryParserName(String queryParserName) {
        this.queryParserName = queryParserName;
        return this;
    }

    String[] types() {
        return this.types;
    }

    public CountRequest types(String... types) {
        this.types = types;
        return this;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        minScore = in.readFloat();
        querySource = in.readUTF();
        if (in.readBoolean()) {
            queryParserName = in.readUTF();
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
        super.writeTo(out);
        out.writeFloat(minScore);
        out.writeUTF(querySource);
        if (queryParserName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryParserName);
        }
        out.writeInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}
