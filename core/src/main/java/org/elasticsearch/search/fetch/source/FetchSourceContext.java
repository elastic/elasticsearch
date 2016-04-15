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

package org.elasticsearch.search.fetch.source;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class FetchSourceContext implements Streamable, ToXContent {

    public static final ParseField INCLUDES_FIELD = new ParseField("includes", "include");
    public static final ParseField EXCLUDES_FIELD = new ParseField("excludes", "exclude");

    public static final FetchSourceContext FETCH_SOURCE = new FetchSourceContext(true);
    public static final FetchSourceContext DO_NOT_FETCH_SOURCE = new FetchSourceContext(false);
    private boolean fetchSource;
    private String[] includes;
    private String[] excludes;

    public static FetchSourceContext parse(QueryParseContext context) throws IOException {
        FetchSourceContext fetchSourceContext = new FetchSourceContext();
        fetchSourceContext.fromXContent(context);
        return fetchSourceContext;
    }

    public FetchSourceContext() {
    }

    public FetchSourceContext(boolean fetchSource) {
        this(fetchSource, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
    }

    public FetchSourceContext(String include) {
        this(include, null);
    }

    public FetchSourceContext(String include, String exclude) {
        this(true,
                include == null ? Strings.EMPTY_ARRAY : new String[]{include},
                exclude == null ? Strings.EMPTY_ARRAY : new String[]{exclude});
    }

    public FetchSourceContext(String[] includes) {
        this(true, includes, Strings.EMPTY_ARRAY);
    }

    public FetchSourceContext(String[] includes, String[] excludes) {
        this(true, includes, excludes);
    }

    public FetchSourceContext(boolean fetchSource, String[] includes, String[] excludes) {
        this.fetchSource = fetchSource;
        this.includes = includes == null ? Strings.EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? Strings.EMPTY_ARRAY : excludes;
    }

    public boolean fetchSource() {
        return this.fetchSource;
    }

    public FetchSourceContext fetchSource(boolean fetchSource) {
        this.fetchSource = fetchSource;
        return this;
    }

    public String[] includes() {
        return this.includes;
    }

    public FetchSourceContext includes(String[] includes) {
        this.includes = includes;
        return this;
    }

    public String[] excludes() {
        return this.excludes;
    }

    public FetchSourceContext excludes(String[] excludes) {
        this.excludes = excludes;
        return this;
    }

    public static FetchSourceContext parseFromRestRequest(RestRequest request) {
        Boolean fetchSource = null;
        String[] source_excludes = null;
        String[] source_includes = null;

        String source = request.param("_source");
        if (source != null) {
            if (Booleans.isExplicitTrue(source)) {
                fetchSource = true;
            } else if (Booleans.isExplicitFalse(source)) {
                fetchSource = false;
            } else {
                source_includes = Strings.splitStringByCommaToArray(source);
            }
        }
        String sIncludes = request.param("_source_includes");
        sIncludes = request.param("_source_include", sIncludes);
        if (sIncludes != null) {
            source_includes = Strings.splitStringByCommaToArray(sIncludes);
        }

        String sExcludes = request.param("_source_excludes");
        sExcludes = request.param("_source_exclude", sExcludes);
        if (sExcludes != null) {
            source_excludes = Strings.splitStringByCommaToArray(sExcludes);
        }

        if (fetchSource != null || source_includes != null || source_excludes != null) {
            return new FetchSourceContext(fetchSource == null ? true : fetchSource, source_includes, source_excludes);
        }
        return null;
    }

    public void fromXContent(QueryParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        boolean fetchSource = true;
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = Strings.EMPTY_ARRAY;
        if (token == XContentParser.Token.VALUE_BOOLEAN) {
            fetchSource = parser.booleanValue();
        } else if (token == XContentParser.Token.VALUE_STRING) {
            includes = new String[]{parser.text()};
        } else if (token == XContentParser.Token.START_ARRAY) {
            ArrayList<String> list = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                list.add(parser.text());
            }
            includes = list.toArray(new String[list.size()]);
        } else if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (context.getParseFieldMatcher().match(currentFieldName, INCLUDES_FIELD)) {
                        List<String> includesList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                includesList.add(parser.text());
                            } else {
                                throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                        parser.getTokenLocation());
                            }
                        }
                        includes = includesList.toArray(new String[includesList.size()]);
                    } else if (context.getParseFieldMatcher().match(currentFieldName, EXCLUDES_FIELD)) {
                        List<String> excludesList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                excludesList.add(parser.text());
                            } else {
                                throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                        parser.getTokenLocation());
                            }
                        }
                        excludes = excludesList.toArray(new String[excludesList.size()]);
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                parser.getTokenLocation());
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (context.getParseFieldMatcher().match(currentFieldName, INCLUDES_FIELD)) {
                        includes = new String[] {parser.text()};
                    } else if (context.getParseFieldMatcher().match(currentFieldName, EXCLUDES_FIELD)) {
                        excludes = new String[] {parser.text()};
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            }
        } else {
            throw new ParsingException(parser.getTokenLocation(), "Expected one of [" + XContentParser.Token.VALUE_BOOLEAN + ", "
                    + XContentParser.Token.START_OBJECT + "] but found [" + token + "]", parser.getTokenLocation());
        }
        this.fetchSource = fetchSource;
        this.includes = includes;
        this.excludes = excludes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (fetchSource) {
            builder.startObject();
            builder.array(INCLUDES_FIELD.getPreferredName(), includes);
            builder.array(EXCLUDES_FIELD.getPreferredName(), excludes);
            builder.endObject();
        } else {
            builder.value(false);
        }
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        fetchSource = in.readBoolean();
        includes = in.readStringArray();
        excludes = in.readStringArray();
        in.readBoolean(); // Used to be transformSource but that was dropped in 2.1
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(fetchSource);
        out.writeStringArray(includes);
        out.writeStringArray(excludes);
        out.writeBoolean(false); // Used to be transformSource but that was dropped in 2.1
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchSourceContext that = (FetchSourceContext) o;

        if (fetchSource != that.fetchSource) return false;
        if (!Arrays.equals(excludes, that.excludes)) return false;
        if (!Arrays.equals(includes, that.includes)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (fetchSource ? 1 : 0);
        result = 31 * result + (includes != null ? Arrays.hashCode(includes) : 0);
        result = 31 * result + (excludes != null ? Arrays.hashCode(excludes) : 0);
        return result;
    }
}
