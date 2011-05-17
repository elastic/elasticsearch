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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.Immutable;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;

/**
 * @author imotov
 */
@Immutable
public class AliasMetaData {

    private final String alias;

    private AliasMetaData(String alias) {
        this.alias = alias;
    }

    public String alias() {
        return alias;
    }

    public String getAlias() {
        return alias();
    }

    public static Builder newAliasMetaDataBuilder(String alias) {
        return new Builder(alias);
    }

    public static class Builder {

        private String alias;

        public Builder(String alias) {
            this.alias = alias;
        }

        public Builder(AliasMetaData aliasMetaData) {
            this(aliasMetaData.alias());
        }

        public String alias() {
            return alias;
        }

        public AliasMetaData build() {
            return new AliasMetaData(alias);
        }

        public static void toXContent(AliasMetaData aliasMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(aliasMetaData.alias(), XContentBuilder.FieldCaseConversion.NONE);
            // Filters will go here
            builder.endObject();
        }

        public static AliasMetaData fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder(parser.currentName());
            XContentParser.Token token = parser.nextToken();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                // Skip the content for now, filter and other settings will go here
            }
            return builder.build();
        }

        public static void writeTo(AliasMetaData aliasMetaData, StreamOutput out) throws IOException {
            out.writeUTF(aliasMetaData.alias());
        }

        public static AliasMetaData readFrom(StreamInput in) throws IOException {
            String alias = in.readUTF();
            return new AliasMetaData(alias);
        }
    }

}
