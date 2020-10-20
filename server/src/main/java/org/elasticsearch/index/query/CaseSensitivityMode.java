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

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.DeprecationHandler;

import java.io.IOException;

/** Case sensitivity matching mode for term-based queries */
public enum CaseSensitivityMode implements Writeable {

    /**
     * use the field's setting for handling matching 
     */
    FIELD_DEFAULT(new ParseField("field_default")),

    /**
     * case insensitive matching
     */
    INSENSITIVE(new ParseField("insensitive"));

    public static final ParseField KEY = new ParseField("case_sensitivity");

    private final ParseField parseField;

    CaseSensitivityMode(ParseField parseField) {
        this.parseField = parseField;
    }

    public ParseField parseField() {
        return parseField;
    }

    public static CaseSensitivityMode parse(String value, DeprecationHandler deprecationHandler) {
        CaseSensitivityMode[] modes = CaseSensitivityMode.values();
        for (CaseSensitivityMode mode : modes) {
            if (mode.parseField.match(value, deprecationHandler)) {
                return mode;
            }
        }
        throw new ElasticsearchParseException("no [{}] found for value [{}]", KEY.getPreferredName(), value);
    }

    public static CaseSensitivityMode readFromStream(StreamInput in) throws IOException {
        return in.readEnum(CaseSensitivityMode.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }
}