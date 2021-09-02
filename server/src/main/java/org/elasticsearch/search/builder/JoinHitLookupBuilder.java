/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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

package org.elasticsearch.search.builder;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A specification to lookup join hits from an external index.
 */
public final class JoinHitLookupBuilder implements Writeable, ToXContent {
    private final String name;
    private final String index;
    private final String matchField;

    // _source filtering
    private String[] storedFields = new String[0];
    private FetchSourceContext fetchSourceContext = new FetchSourceContext(true);

    /**
     * Create a specification to lookup join hits from an external index
     *
     * @param name       the name of the group of the inner hits
     * @param index      the name of the external index
     * @param matchField the name of the field whose values are used as the lookup id
     */
    public JoinHitLookupBuilder(String name, String index, String matchField) {
        this.name = Objects.requireNonNull(name, "[name] parameter must be non-null");
        this.index = Objects.requireNonNull(index, "[index] parameter must be non-null");
        this.matchField = Objects.requireNonNull(matchField, "[match_field] parameter must be null");
    }

    JoinHitLookupBuilder(StreamInput in) throws IOException {
        this.name = in.readString();
        this.index = in.readString();
        this.matchField = in.readString();
        this.storedFields = in.readOptionalStringArray();
        this.fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(index);
        out.writeString(matchField);
        out.writeOptionalStringArray(storedFields);
        out.writeOptionalWriteable(fetchSourceContext);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("index", index);
        builder.field("match_field", matchField);
        if (storedFields != null && storedFields.length > 0) {
            builder.startArray("stored_fields");
            for (String field : storedFields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (fetchSourceContext != null) {
            builder.field("_source", fetchSourceContext);
        }
        builder.endObject();
        return builder;
    }

    public static JoinHitLookupBuilder fromXContent(XContentParser parser) throws IOException {
        String name = null;
        String index = null;
        String matchField = null;
        String[] storedFields = null;
        FetchSourceContext fetchSourceContext = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                assert currentFieldName != null;
                switch (currentFieldName) {
                    case "name":
                        name = parser.text();
                        break;
                    case "index":
                        index = parser.text();
                        break;
                    case "match_field":
                        matchField = parser.text();
                        break;
                    case "stored_fields":
                        if (token == XContentParser.Token.START_ARRAY) {
                            final List<String> fields = new ArrayList<>();
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                fields.add(parser.text());
                            }
                            storedFields = fields.toArray(String[]::new);
                        } else {
                            storedFields = Strings.splitStringByCommaToArray(parser.text());
                        }
                        break;
                    case "_source":
                        fetchSourceContext = FetchSourceContext.fromXContent(parser);
                        break;
                    default:
                        throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                }
            }
        }
         if (Strings.isEmpty(name)) {
            throw new IllegalArgumentException("[name] of a join hit must be provided");
        }
        if (Strings.isEmpty(index)) {
            throw new IllegalArgumentException("[index] of a join hit must be provided");
        }
        if (Strings.isEmpty(matchField)) {
            throw new IllegalArgumentException("[match_field] of a join hit must be provided");
        }
        final JoinHitLookupBuilder joinHitLookupBuilder = new JoinHitLookupBuilder(name, index, matchField);
        if (storedFields != null) {
            joinHitLookupBuilder.setStoredFields(storedFields);
        }
        if (fetchSourceContext != null) {
            joinHitLookupBuilder.setFetchSourceContext(fetchSourceContext);
        }
        return joinHitLookupBuilder;
    }

    public String getName() {
        return name;
    }

    public String getIndex() {
        return index;
    }

    public String getMatchField() {
        return matchField;
    }

    public String[] getStoredFields() {
        return storedFields;
    }

    public void setStoredFields(String[] storedFields) {
        this.storedFields = storedFields;
    }

    public FetchSourceContext getFetchSourceContext() {
        return fetchSourceContext;
    }

    public void setFetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinHitLookupBuilder that = (JoinHitLookupBuilder) o;
        return name.equals(that.name) && index.equals(that.index) && matchField.equals(that.matchField)
            && Arrays.equals(storedFields, that.storedFields) && fetchSourceContext.equals(that.fetchSourceContext);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, index, matchField, fetchSourceContext);
        result = 31 * result + Arrays.hashCode(storedFields);
        return result;
    }
}
