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

package org.elasticsearch.action.termvector;

import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Request returning the term vector (doc frequency, positions, offsets) for a
 * document.
 * <p/>
 * Note, the {@link #index()}, {@link #type(String)} and {@link #id(String)} are
 * required.
 */
public class TermVectorRequest extends SingleShardOperationRequest<TermVectorRequest> {

    private String type;

    private String id;

    private BytesReference doc;

    private String routing;

    protected String preference;

    private static final AtomicInteger randomInt = new AtomicInteger(0);

    // TODO: change to String[]
    private Set<String> selectedFields;

    private EnumSet<Flag> flagsEnum = EnumSet.of(Flag.Positions, Flag.Offsets, Flag.Payloads,
            Flag.FieldStatistics);

    public TermVectorRequest() {
    }

    /**
     * Constructs a new term vector request for a document that will be fetch
     * from the provided index. Use {@link #type(String)} and
     * {@link #id(String)} to specify the document to load.
     */
    public TermVectorRequest(String index, String type, String id) {
        super(index);
        this.id = id;
        this.type = type;
    }

    /**
     * Constructs a new term vector request for a document that will be fetch
     * from the provided index. Use {@link #type(String)} and
     * {@link #id(String)} to specify the document to load.
     */
    public TermVectorRequest(TermVectorRequest other) {
        super(other.index());
        this.id = other.id();
        this.type = other.type();
        this.flagsEnum = other.getFlags().clone();
        this.preference = other.preference();
        this.routing = other.routing();
        if (other.selectedFields != null) {
            this.selectedFields = new HashSet<>(other.selectedFields);
        }
    }

    public TermVectorRequest(MultiGetRequest.Item item) {
        super(item.index());
        this.id = item.id();
        this.type = item.type();
        this.selectedFields(item.fields());
        this.routing(item.routing());
    }

    public EnumSet<Flag> getFlags() {
        return flagsEnum;
    }

    /**
     * Sets the type of document to get the term vector for.
     */
    public TermVectorRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Returns the type of document to get the term vector for.
     */
    public String type() {
        return type;
    }

    /**
     * Returns the id of document the term vector is requested for.
     */
    public String id() {
        return id;
    }
    
    /**
     * Sets the id of document the term vector is requested for.
     */
    public TermVectorRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Returns the artificial document from which term vectors are requested for.
     */
    public BytesReference doc() {
        return doc;
    }

    /**
     * Sets an artificial document from which term vectors are requested for.
     */
    public TermVectorRequest doc(XContentBuilder documentBuilder) {
        // assign a random id to this artificial document, for routing
        this.id(String.valueOf(randomInt.getAndAdd(1)));
        this.doc = documentBuilder.bytes();
        return this;
    }

    /**
     * @return The routing for this request.
     */
    public String routing() {
        return routing;
    }

    public TermVectorRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the parent id of this document. Will simply set the routing to this
     * value, as it is only used for routing with delete requests.
     */
    public TermVectorRequest parent(String parent) {
        if (routing == null) {
            routing = parent;
        }
        return this;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across
     * shards. Can be set to <tt>_local</tt> to prefer local shards,
     * <tt>_primary</tt> to execute only on primary shards, or a custom value,
     * which guarantees that the same order will be used across different
     * requests.
     */
    public TermVectorRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * Return the start and stop offsets for each term if they were stored or
     * skip offsets.
     */
    public TermVectorRequest offsets(boolean offsets) {
        setFlag(Flag.Offsets, offsets);
        return this;
    }

    /**
     * @return <code>true</code> if term offsets should be returned. Otherwise
     * <code>false</code>
     */
    public boolean offsets() {
        return flagsEnum.contains(Flag.Offsets);
    }

    /**
     * Return the positions for each term if stored or skip.
     */
    public TermVectorRequest positions(boolean positions) {
        setFlag(Flag.Positions, positions);
        return this;
    }

    /**
     * @return Returns if the positions for each term should be returned if
     *         stored or skip.
     */
    public boolean positions() {
        return flagsEnum.contains(Flag.Positions);
    }

    /**
     * @return <code>true</code> if term payloads should be returned. Otherwise
     * <code>false</code>
     */
    public boolean payloads() {
        return flagsEnum.contains(Flag.Payloads);
    }

    /**
     * Return the payloads for each term or skip.
     */
    public TermVectorRequest payloads(boolean payloads) {
        setFlag(Flag.Payloads, payloads);
        return this;
    }

    /**
     * @return <code>true</code> if term statistics should be returned.
     * Otherwise <code>false</code>
     */
    public boolean termStatistics() {
        return flagsEnum.contains(Flag.TermStatistics);
    }

    /**
     * Return the term statistics for each term in the shard or skip.
     */
    public TermVectorRequest termStatistics(boolean termStatistics) {
        setFlag(Flag.TermStatistics, termStatistics);
        return this;
    }

    /**
     * @return <code>true</code> if field statistics should be returned.
     * Otherwise <code>false</code>
     */
    public boolean fieldStatistics() {
        return flagsEnum.contains(Flag.FieldStatistics);
    }

    /**
     * Return the field statistics for each term in the shard or skip.
     */
    public TermVectorRequest fieldStatistics(boolean fieldStatistics) {
        setFlag(Flag.FieldStatistics, fieldStatistics);
        return this;
    }

    /**
     * Return only term vectors for special selected fields. Returns for term
     * vectors for all fields if selectedFields == null
     */
    public Set<String> selectedFields() {
        return selectedFields;
    }

    /**
     * Return only term vectors for special selected fields. Returns the term
     * vectors for all fields if selectedFields == null
     */
    public TermVectorRequest selectedFields(String[] fields) {
        selectedFields = fields != null && fields.length != 0 ? Sets.newHashSet(fields) : null;
        return this;
    }

    private void setFlag(Flag flag, boolean set) {
        if (set && !flagsEnum.contains(flag)) {
            flagsEnum.add(flag);
        } else if (!set) {
            flagsEnum.remove(flag);
            assert (!flagsEnum.contains(flag));
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = ValidateActions.addValidationError("type is missing", validationException);
        }
        if (id == null && doc == null) {
            validationException = ValidateActions.addValidationError("id or doc is missing", validationException);
        }
        return validationException;
    }

    public static TermVectorRequest readTermVectorRequest(StreamInput in) throws IOException {
        TermVectorRequest termVectorRequest = new TermVectorRequest();
        termVectorRequest.readFrom(in);
        return termVectorRequest;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().before(Version.V_1_4_0_Beta1)) {
            //term vector used to read & write the index twice, here and in the parent class
            in.readString();
        }
        type = in.readString();
        id = in.readString();

        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            if (in.readBoolean()) {
                doc = in.readBytesReference();
            }
        }
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        long flags = in.readVLong();

        flagsEnum.clear();
        for (Flag flag : Flag.values()) {
            if ((flags & (1 << flag.ordinal())) != 0) {
                flagsEnum.add(flag);
            }
        }
        int numSelectedFields = in.readVInt();
        if (numSelectedFields > 0) {
            selectedFields = new HashSet<>();
            for (int i = 0; i < numSelectedFields; i++) {
                selectedFields.add(in.readString());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_1_4_0_Beta1)) {
            //term vector used to read & write the index twice, here and in the parent class
            out.writeString(index);
        }
        out.writeString(type);
        out.writeString(id);

        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeBoolean(doc != null);
            if (doc != null) {
                out.writeBytesReference(doc);
            }
        }
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        long longFlags = 0;
        for (Flag flag : flagsEnum) {
            longFlags |= (1 << flag.ordinal());
        }
        out.writeVLong(longFlags);
        if (selectedFields != null) {
            out.writeVInt(selectedFields.size());
            for (String selectedField : selectedFields) {
                out.writeString(selectedField);
            }
        } else {
            out.writeVInt(0);
        }
    }

    public static enum Flag {
        // Do not change the order of these flags we use
        // the ordinal for encoding! Only append to the end!
        Positions, Offsets, Payloads, FieldStatistics, TermStatistics
    }

    /**
     * populates a request object (pre-populated with defaults) based on a parser.
     */
    public static void parseRequest(TermVectorRequest termVectorRequest, XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        List<String> fields = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (currentFieldName != null) {
                if (currentFieldName.equals("fields")) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            fields.add(parser.text());
                        }
                    } else {
                        throw new ElasticsearchParseException(
                                "The parameter fields must be given as an array! Use syntax : \"fields\" : [\"field1\", \"field2\",...]");
                    }
                } else if (currentFieldName.equals("offsets")) {
                    termVectorRequest.offsets(parser.booleanValue());
                } else if (currentFieldName.equals("positions")) {
                    termVectorRequest.positions(parser.booleanValue());
                } else if (currentFieldName.equals("payloads")) {
                    termVectorRequest.payloads(parser.booleanValue());
                } else if (currentFieldName.equals("term_statistics") || currentFieldName.equals("termStatistics")) {
                    termVectorRequest.termStatistics(parser.booleanValue());
                } else if (currentFieldName.equals("field_statistics") || currentFieldName.equals("fieldStatistics")) {
                    termVectorRequest.fieldStatistics(parser.booleanValue());
                } else if ("_index".equals(currentFieldName)) { // the following is important for multi request parsing.
                    termVectorRequest.index = parser.text();
                } else if ("_type".equals(currentFieldName)) {
                    termVectorRequest.type = parser.text();
                } else if ("_id".equals(currentFieldName)) {
                    if (termVectorRequest.doc != null) {
                        throw new ElasticsearchParseException("Either \"id\" or \"doc\" can be specified, but not both!");
                    }
                    termVectorRequest.id = parser.text();
                } else if ("doc".equals(currentFieldName)) {
                    if (termVectorRequest.id != null) {
                        throw new ElasticsearchParseException("Either \"id\" or \"doc\" can be specified, but not both!");
                    }
                    termVectorRequest.doc(jsonBuilder().copyCurrentStructure(parser));
                } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                    termVectorRequest.routing = parser.text();
                } else {
                    throw new ElasticsearchParseException("The parameter " + currentFieldName
                            + " is not valid for term vector request!");
                }
            }
        }
        if (fields.size() > 0) {
            String[] fieldsAsArray = new String[fields.size()];
            termVectorRequest.selectedFields(fields.toArray(fieldsAsArray));
        }
    }
}
