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
package org.elasticsearch.client.textstructure;

import org.elasticsearch.client.textstructure.structurefinder.TextStructure;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class FindStructureResponse implements ToXContentObject {

    private final TextStructure textStructure;

    FindStructureResponse(TextStructure textStructure) {
        this.textStructure = Objects.requireNonNull(textStructure);
    }

    public static FindStructureResponse fromXContent(XContentParser parser) throws IOException {
        return new FindStructureResponse(TextStructure.PARSER.parse(parser, null).build());
    }

    public TextStructure getFileStructure() {
        return textStructure;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        textStructure.toXContent(builder, params);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(textStructure);
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FindStructureResponse that = (FindStructureResponse) other;
        return Objects.equals(textStructure, that.textStructure);
    }
}
