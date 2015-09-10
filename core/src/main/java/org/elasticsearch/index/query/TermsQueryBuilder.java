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

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.cache.query.terms.TermsLookup;

import java.io.IOException;
import java.util.*;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class TermsQueryBuilder extends AbstractQueryBuilder<TermsQueryBuilder> {

    public static final String NAME = "terms";

    static final TermsQueryBuilder PROTOTYPE = new TermsQueryBuilder("");

    public static final boolean DEFAULT_DISABLE_COORD = false;

    private final String fieldName;
    private final List<Object> values;
    @Deprecated
    private String minimumShouldMatch;
    @Deprecated
    private boolean disableCoord = DEFAULT_DISABLE_COORD;
    private TermsLookup termsLookup;

    TermsQueryBuilder(String fieldName, List<Object> values, String minimumShouldMatch, boolean disableCoord, TermsLookup termsLookup) {
        this.fieldName = fieldName;
        if (values == null && termsLookup == null) {
            throw new IllegalArgumentException("No value specified for terms query");
        }
        this.values = values;
        this.disableCoord = disableCoord;
        this.minimumShouldMatch = minimumShouldMatch;
        this.termsLookup = termsLookup;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, String... values) {
        this(fieldName, values != null ? Arrays.asList(values) : (Iterable<?>) null);
    }
    
    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, int... values) {
        this(fieldName, values != null ? Ints.asList(values) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, long... values) {
        this(fieldName, values != null ? Longs.asList(values) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, float... values) {
        this(fieldName, values != null ? Floats.asList(values) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, double... values) {
        this(fieldName, values != null ? Doubles.asList(values) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, Object... values) {
        this(fieldName, values != null ? Arrays.asList(values) : (Iterable<?>) null);
    }

    /**
     * Constructor used for terms query lookup.
     *
     * @param fieldName The field name
     */
    public TermsQueryBuilder(String fieldName) {
        this.fieldName = fieldName;
        this.values = null;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, Iterable<?> values) {
        if (values == null) {
            throw new IllegalArgumentException("No value specified for terms query");
        }
        this.fieldName = fieldName;
        this.values = convertToBytesRefListIfStringList(values);
    }

    public String fieldName() {
        return this.fieldName;
    }

    public List<Object> values() {
        return convertToStringListIfBytesRefList(this.values);
    }

    /**
     * Sets the minimum number of matches across the provided terms. Defaults to <tt>1</tt>.
     * @deprecated use [bool] query instead
     */
    @Deprecated
    public TermsQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public String minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    /**
     * Disables <tt>Similarity#coord(int,int)</tt> in scoring. Defaults to <tt>false</tt>.
     * @deprecated use [bool] query instead
     */
    @Deprecated
    public TermsQueryBuilder disableCoord(boolean disableCoord) {
        this.disableCoord = disableCoord;
        return this;
    }

    boolean disableCoord() {
        return this.disableCoord;
    }

    private boolean isTermsLookupQuery() {
        return this.termsLookup != null;
    }

    public TermsQueryBuilder termsLookup(TermsLookup termsLookup) {
        this.termsLookup = termsLookup;
        return this;
    }

    public TermsLookup termsLookup() {
        return this.termsLookup;
    }

    /**
     * Sets the index name to lookup the terms from.
     */
    public TermsQueryBuilder lookupIndex(String lookupIndex) {
        if (lookupIndex == null) {
            throw new IllegalArgumentException("Lookup index cannot be set to null");
        }
        if (this.termsLookup == null) {
            this.termsLookup = new TermsLookup();
        }
        this.termsLookup.index(lookupIndex);
        return this;
    }

    /**
     * Sets the type name to lookup the terms from.
     */
    public TermsQueryBuilder lookupType(String lookupType) {
        if (lookupType == null) {
            throw new IllegalArgumentException("Lookup type cannot be set to null");
        }
        if (this.termsLookup == null) {
            this.termsLookup = new TermsLookup();
        }
        this.termsLookup.type(lookupType);
        return this;
    }

    /**
     * Sets the document id to lookup the terms from.
     */
    public TermsQueryBuilder lookupId(String lookupId) {
        if (lookupId == null) {
            throw new IllegalArgumentException("Lookup id cannot be set to null");
        }
        if (this.termsLookup == null) {
            this.termsLookup = new TermsLookup();
        }
        this.termsLookup.id(lookupId);
        return this;
    }

    /**
     * Sets the path name to lookup the terms from.
     */
    public TermsQueryBuilder lookupPath(String lookupPath) {
        if (lookupPath == null) {
            throw new IllegalArgumentException("Lookup path cannot be set to null");
        }
        if (this.termsLookup == null) {
            this.termsLookup = new TermsLookup();
        }
        this.termsLookup.path(lookupPath);
        return this;
    }

    /**
     * Sets the routing to lookup the terms from.
     */
    public TermsQueryBuilder lookupRouting(String lookupRouting) {
        if (lookupRouting == null) {
            throw new IllegalArgumentException("Lookup routing cannot be set to null");
        }
        if (this.termsLookup == null) {
            this.termsLookup = new TermsLookup();
        }
        this.termsLookup.routing(lookupRouting);
        return this;
    }

    /**
     * Same as {@link #convertToBytesRefIfString} but on Iterable.
     * @param objs the Iterable of input object
     * @return the same input or a list of {@link BytesRef} representation if input was a list of type string
     */
    private static List<Object> convertToBytesRefListIfStringList(Iterable<?> objs) {
        if (objs == null) {
            return null;
        }
        List<Object> newObjs = new ArrayList<>();
        for (Object obj : objs) {
            newObjs.add(convertToBytesRefIfString(obj));
        }
        return newObjs;
    }

    /**
     * Same as {@link #convertToStringIfBytesRef} but on Iterable.
     * @param objs the Iterable of input object
     * @return the same input or a list of utf8 string if input was a list of type {@link BytesRef}
     */
    private static List<Object> convertToStringListIfBytesRefList(Iterable<?> objs) {
        if (objs == null) {
            return null;
        }
        List<Object> newObjs = new ArrayList<>();
        for (Object obj : objs) {
            newObjs.add(convertToStringIfBytesRef(obj));
        }
        return newObjs;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (isTermsLookupQuery()) {
            builder.startObject(fieldName);
            termsLookup.toXContent(builder, params);
            builder.endObject();
        } else {
            builder.field(fieldName, convertToStringListIfBytesRefList(values));
        }
        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }
        if (disableCoord != DEFAULT_DISABLE_COORD) {
            builder.field("disable_coord", disableCoord);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        List<Object> terms;
        if (isTermsLookupQuery()) {
            if (termsLookup.index() == null) {
                termsLookup.index(context.index().name());
            }
            terms = context.handleTermsLookup(termsLookup);
        } else {
            terms = values;
        }
        if (terms == null || terms.isEmpty()) {
            return Queries.newMatchNoDocsQuery();
        }
        return handleTermsQuery(terms, fieldName, context, minimumShouldMatch, disableCoord);
    }

    private static Query handleTermsQuery(List<Object> terms, String fieldName, QueryShardContext context, String minimumShouldMatch, boolean disableCoord) {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        String indexFieldName;
        if (fieldType != null) {
            indexFieldName = fieldType.names().indexName();
        } else {
            indexFieldName = fieldName;
        }

        Query query;
        if (context.isFilter()) {
            if (fieldType != null) {
                query = fieldType.termsQuery(terms, context);
            } else {
                BytesRef[] filterValues = new BytesRef[terms.size()];
                for (int i = 0; i < filterValues.length; i++) {
                    filterValues[i] = BytesRefs.toBytesRef(terms.get(i));
                }
                query = new TermsQuery(indexFieldName, filterValues);
            }
        } else {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.setDisableCoord(disableCoord);
            for (Object term : terms) {
                if (fieldType != null) {
                    bq.add(fieldType.termQuery(term, context), BooleanClause.Occur.SHOULD);
                } else {
                    bq.add(new TermQuery(new Term(indexFieldName, BytesRefs.toBytesRef(term))), BooleanClause.Occur.SHOULD);
                }
            }
            query = Queries.applyMinimumShouldMatch(bq.build(), minimumShouldMatch);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (this.fieldName == null) {
            validationException = addValidationError("field name cannot be null.", validationException);
        }
        if (isTermsLookupQuery() && this.values != null) {
            validationException = addValidationError("can't have both a terms query and a lookup query.", validationException);
        }
        if (isTermsLookupQuery()) {
            QueryValidationException exception = termsLookup.validate();
            if (exception != null) {
                validationException = QueryValidationException.addValidationErrors(exception.validationErrors(), validationException);
            }
        }
        return validationException;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected TermsQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String field = in.readString();
        TermsLookup lookup = null;
        if (in.readBoolean()) {
            lookup = TermsLookup.readTermsLookupFrom(in);
        }
        List<Object> values = (List<Object>) in.readGenericValue();
        String minimumShouldMatch = in.readOptionalString();
        boolean disableCoord = in.readBoolean();
        return new TermsQueryBuilder(field, values, minimumShouldMatch, disableCoord, lookup);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeBoolean(isTermsLookupQuery());
        if (isTermsLookupQuery()) {
            termsLookup.writeTo(out);
        }
        out.writeGenericValue(values);
        out.writeOptionalString(minimumShouldMatch);
        out.writeBoolean(disableCoord);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, values, minimumShouldMatch, disableCoord, termsLookup);
    }

    @Override
    protected boolean doEquals(TermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(values, other.values) &&
                Objects.equals(minimumShouldMatch, other.minimumShouldMatch) &&
                Objects.equals(disableCoord, other.disableCoord) &&
                Objects.equals(termsLookup, other.termsLookup);
    }
}
