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

package org.elasticsearch.action.termvectors;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Request returning the term vector (doc frequency, positions, offsets) for a
 * document.
 * <p>
 * Note, the {@link #index()}, {@link #type(String)} and {@link #id(String)} are
 * required.
 */
public class TermVectorsRequest extends SingleShardRequest<TermVectorsRequest> implements DocumentRequest<TermVectorsRequest>, RealtimeRequest {

    private String type;

    private String id;

    private BytesReference doc;

    private String routing;

    private String parent;

    private VersionType versionType = VersionType.INTERNAL;

    private long version = Versions.MATCH_ANY;

    protected String preference;

    private static final AtomicInteger randomInt = new AtomicInteger(0);

    // TODO: change to String[]
    private Set<String> selectedFields;

    Boolean realtime;

    private Map<String, String> perFieldAnalyzer;

    private FilterSettings filterSettings;

    public static final class FilterSettings {
        public Integer maxNumTerms;
        public Integer minTermFreq;
        public Integer maxTermFreq;
        public Integer minDocFreq;
        public Integer maxDocFreq;
        public Integer minWordLength;
        public Integer maxWordLength;

        public FilterSettings() {

        }

        public FilterSettings(@Nullable Integer maxNumTerms, @Nullable Integer minTermFreq, @Nullable Integer maxTermFreq,
                              @Nullable Integer minDocFreq, @Nullable Integer maxDocFreq, @Nullable Integer minWordLength,
                              @Nullable Integer maxWordLength) {
            this.maxNumTerms = maxNumTerms;
            this.minTermFreq = minTermFreq;
            this.maxTermFreq = maxTermFreq;
            this.minDocFreq = minDocFreq;
            this.maxDocFreq = maxDocFreq;
            this.minWordLength = minWordLength;
            this.maxWordLength = maxWordLength;
        }

        public void readFrom(StreamInput in) throws IOException {
            maxNumTerms = in.readOptionalVInt();
            minTermFreq = in.readOptionalVInt();
            maxTermFreq = in.readOptionalVInt();
            minDocFreq = in.readOptionalVInt();
            maxDocFreq = in.readOptionalVInt();
            minWordLength = in.readOptionalVInt();
            maxWordLength = in.readOptionalVInt();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(maxNumTerms);
            out.writeOptionalVInt(minTermFreq);
            out.writeOptionalVInt(maxTermFreq);
            out.writeOptionalVInt(minDocFreq);
            out.writeOptionalVInt(maxDocFreq);
            out.writeOptionalVInt(minWordLength);
            out.writeOptionalVInt(maxWordLength);
        }
    }

    private EnumSet<Flag> flagsEnum = EnumSet.of(Flag.Positions, Flag.Offsets, Flag.Payloads,
            Flag.FieldStatistics);

    long startTime;

    public TermVectorsRequest() {
    }

    /**
     * Constructs a new term vector request for a document that will be fetch
     * from the provided index. Use {@link #type(String)} and
     * {@link #id(String)} to specify the document to load.
     */
    public TermVectorsRequest(String index, String type, String id) {
        super(index);
        this.id = id;
        this.type = type;
    }

    /**
     * Constructs a new term vector request for a document that will be fetch
     * from the provided index. Use {@link #type(String)} and
     * {@link #id(String)} to specify the document to load.
     */
    public TermVectorsRequest(TermVectorsRequest other) {
        super(other.index());
        this.id = other.id();
        this.type = other.type();
        if (this.doc != null) {
            this.doc = other.doc().copyBytesArray();
        }
        this.flagsEnum = other.getFlags().clone();
        this.preference = other.preference();
        this.routing = other.routing();
        this.parent = other.parent();
        if (other.selectedFields != null) {
            this.selectedFields = new HashSet<>(other.selectedFields);
        }
        if (other.perFieldAnalyzer != null) {
            this.perFieldAnalyzer = new HashMap<>(other.perFieldAnalyzer);
        }
        this.realtime = other.realtime();
        this.version = other.version();
        this.versionType = VersionType.fromValue(other.versionType().getValue());
        this.startTime = other.startTime();
        this.filterSettings = other.filterSettings();
    }

    public TermVectorsRequest(MultiGetRequest.Item item) {
        super(item.index());
        this.id = item.id();
        this.type = item.type();
        this.selectedFields(item.fields());
        this.routing(item.routing());
        this.parent(item.parent());
    }

    public EnumSet<Flag> getFlags() {
        return flagsEnum;
    }

    /**
     * Sets the type of document to get the term vector for.
     */
    public TermVectorsRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Returns the type of document to get the term vector for.
     */
    @Override
    public String type() {
        return type;
    }

    /**
     * Returns the id of document the term vector is requested for.
     */
    @Override
    public String id() {
        return id;
    }

    /**
     * Sets the id of document the term vector is requested for.
     */
    public TermVectorsRequest id(String id) {
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
    public TermVectorsRequest doc(XContentBuilder documentBuilder) {
        return this.doc(documentBuilder.bytes(), true);
    }

    /**
     * Sets an artificial document from which term vectors are requested for.
     */
    public TermVectorsRequest doc(BytesReference doc, boolean generateRandomId) {
        // assign a random id to this artificial document, for routing
        if (generateRandomId) {
            this.id(String.valueOf(randomInt.getAndAdd(1)));
        }
        this.doc = doc;
        return this;
    }

    /**
     * @return The routing for this request.
     */
    @Override
    public String routing() {
        return routing;
    }

    @Override
    public TermVectorsRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    @Override
    public String parent() {
        return parent;
    }

    /**
     * Sets the parent id of this document.
     */
    public TermVectorsRequest parent(String parent) {
        this.parent = parent;
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
    public TermVectorsRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * Return the start and stop offsets for each term if they were stored or
     * skip offsets.
     */
    public TermVectorsRequest offsets(boolean offsets) {
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
    public TermVectorsRequest positions(boolean positions) {
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
    public TermVectorsRequest payloads(boolean payloads) {
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
    public TermVectorsRequest termStatistics(boolean termStatistics) {
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
    public TermVectorsRequest fieldStatistics(boolean fieldStatistics) {
        setFlag(Flag.FieldStatistics, fieldStatistics);
        return this;
    }

    /**
     * @return <code>true</code> if distributed frequencies should be returned. Otherwise
     * <code>false</code>
     */
    public boolean dfs() {
        return flagsEnum.contains(Flag.Dfs);
    }

    /**
     * Use distributed frequencies instead of shard statistics.
     */
    public TermVectorsRequest dfs(boolean dfs) {
        setFlag(Flag.Dfs, dfs);
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
    public TermVectorsRequest selectedFields(String... fields) {
        selectedFields = fields != null && fields.length != 0 ? Sets.newHashSet(fields) : null;
        return this;
    }

    /**
     * Return whether term vectors should be generated real-time (default to true).
     */
    public boolean realtime() {
        return this.realtime == null ? true : this.realtime;
    }

    /**
     * Choose whether term vectors be generated real-time.
     */
    @Override
    public TermVectorsRequest realtime(Boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    /**
     * Return the overridden analyzers at each field.
     */
    public Map<String, String> perFieldAnalyzer() {
        return perFieldAnalyzer;
    }

    /**
     * Override the analyzer used at each field when generating term vectors.
     */
    public TermVectorsRequest perFieldAnalyzer(Map<String, String> perFieldAnalyzer) {
        this.perFieldAnalyzer = perFieldAnalyzer != null && perFieldAnalyzer.size() != 0 ? new HashMap<>(perFieldAnalyzer) : null;
        return this;
    }

    /**
     * Return the settings for filtering out terms.
     */
    public FilterSettings filterSettings() {
        return this.filterSettings;
    }

    /**
     * Sets the settings for filtering out terms.
     */
    public TermVectorsRequest filterSettings(FilterSettings settings) {
        this.filterSettings = settings != null ? settings : null;
        return this;
    }

    public long version() {
        return version;
    }

    public TermVectorsRequest version(long version) {
        this.version = version;
        return this;
    }

    public VersionType versionType() {
        return versionType;
    }

    public TermVectorsRequest versionType(VersionType versionType) {
        this.versionType = versionType;
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

    public long startTime() {
        return this.startTime;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validateNonNullIndex();
        if (type == null) {
            validationException = ValidateActions.addValidationError("type is missing", validationException);
        }
        if (id == null && doc == null) {
            validationException = ValidateActions.addValidationError("id or doc is missing", validationException);
        }
        return validationException;
    }

    public static TermVectorsRequest readTermVectorsRequest(StreamInput in) throws IOException {
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest();
        termVectorsRequest.readFrom(in);
        return termVectorsRequest;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readString();
        id = in.readString();

        if (in.readBoolean()) {
            doc = in.readBytesReference();
        }
        routing = in.readOptionalString();
        parent = in.readOptionalString();
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
        if (in.readBoolean()) {
            perFieldAnalyzer = readPerFieldAnalyzer(in.readMap());
        }
        if (in.readBoolean()) {
            filterSettings = new FilterSettings();
            filterSettings.readFrom(in);
        }
        realtime = in.readBoolean();
        versionType = VersionType.fromValue(in.readByte());
        version = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(type);
        out.writeString(id);

        out.writeBoolean(doc != null);
        if (doc != null) {
            out.writeBytesReference(doc);
        }
        out.writeOptionalString(routing);
        out.writeOptionalString(parent);
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
        out.writeBoolean(perFieldAnalyzer != null);
        if (perFieldAnalyzer != null) {
            out.writeGenericValue(perFieldAnalyzer);
        }
        out.writeBoolean(filterSettings != null);
        if (filterSettings != null) {
            filterSettings.writeTo(out);
        }
        out.writeBoolean(realtime());
        out.writeByte(versionType.getValue());
        out.writeLong(version);
    }

    public static enum Flag {
        // Do not change the order of these flags we use
        // the ordinal for encoding! Only append to the end!
        Positions, Offsets, Payloads, FieldStatistics, TermStatistics, Dfs
    }

    /**
     * populates a request object (pre-populated with defaults) based on a parser.
     */
    public static void parseRequest(TermVectorsRequest termVectorsRequest, XContentParser parser) throws IOException {
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
                        throw new ElasticsearchParseException("failed to parse term vectors request. field [fields] must be an array");
                    }
                } else if (currentFieldName.equals("offsets")) {
                    termVectorsRequest.offsets(parser.booleanValue());
                } else if (currentFieldName.equals("positions")) {
                    termVectorsRequest.positions(parser.booleanValue());
                } else if (currentFieldName.equals("payloads")) {
                    termVectorsRequest.payloads(parser.booleanValue());
                } else if (currentFieldName.equals("term_statistics") || currentFieldName.equals("termStatistics")) {
                    termVectorsRequest.termStatistics(parser.booleanValue());
                } else if (currentFieldName.equals("field_statistics") || currentFieldName.equals("fieldStatistics")) {
                    termVectorsRequest.fieldStatistics(parser.booleanValue());
                } else if (currentFieldName.equals("dfs")) {
                    termVectorsRequest.dfs(parser.booleanValue());
                } else if (currentFieldName.equals("per_field_analyzer") || currentFieldName.equals("perFieldAnalyzer")) {
                    termVectorsRequest.perFieldAnalyzer(readPerFieldAnalyzer(parser.map()));
                } else if (currentFieldName.equals("filter")) {
                    termVectorsRequest.filterSettings(readFilterSettings(parser, termVectorsRequest));
                } else if ("_index".equals(currentFieldName)) { // the following is important for multi request parsing.
                    termVectorsRequest.index = parser.text();
                } else if ("_type".equals(currentFieldName)) {
                    termVectorsRequest.type = parser.text();
                } else if ("_id".equals(currentFieldName)) {
                    if (termVectorsRequest.doc != null) {
                        throw new ElasticsearchParseException("failed to parse term vectors request. either [id] or [doc] can be specified, but not both!");
                    }
                    termVectorsRequest.id = parser.text();
                } else if ("doc".equals(currentFieldName)) {
                    if (termVectorsRequest.id != null) {
                        throw new ElasticsearchParseException("failed to parse term vectors request. either [id] or [doc] can be specified, but not both!");
                    }
                    termVectorsRequest.doc(jsonBuilder().copyCurrentStructure(parser));
                } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                    termVectorsRequest.routing = parser.text();
                } else if ("_parent".equals(currentFieldName) || "parent".equals(currentFieldName)) {
                    termVectorsRequest.parent = parser.text();
                } else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
                    termVectorsRequest.version = parser.longValue();
                } else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName) || "version_type".equals(currentFieldName) || "versionType".equals(currentFieldName)) {
                    termVectorsRequest.versionType = VersionType.fromString(parser.text());
                } else {
                    throw new ElasticsearchParseException("failed to parse term vectors request. unknown field [{}]", currentFieldName);
                }
            }
        }
        if (fields.size() > 0) {
            String[] fieldsAsArray = new String[fields.size()];
            termVectorsRequest.selectedFields(fields.toArray(fieldsAsArray));
        }
    }

    public static Map<String, String> readPerFieldAnalyzer(Map<String, Object> map) {
        Map<String, String> mapStrStr = new HashMap<>();
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (e.getValue() instanceof String) {
                mapStrStr.put(e.getKey(), (String) e.getValue());
            } else {
                throw new ElasticsearchParseException("expecting the analyzer at [{}] to be a String, but found [{}] instead", e.getKey(), e.getValue().getClass());
            }
        }
        return mapStrStr;
    }

    private static FilterSettings readFilterSettings(XContentParser parser, TermVectorsRequest termVectorsRequest) throws IOException {
        FilterSettings settings = new FilterSettings();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (currentFieldName != null) {
                if (currentFieldName.equals("max_num_terms")) {
                    settings.maxNumTerms = parser.intValue();
                } else if (currentFieldName.equals("min_term_freq")) {
                    settings.minTermFreq = parser.intValue();
                } else if (currentFieldName.equals("max_term_freq")) {
                    settings.maxTermFreq = parser.intValue();
                } else if (currentFieldName.equals("min_doc_freq")) {
                    settings.minDocFreq = parser.intValue();
                } else if (currentFieldName.equals("max_doc_freq")) {
                    settings.maxDocFreq = parser.intValue();
                } else if (currentFieldName.equals("min_word_length")) {
                    settings.minWordLength = parser.intValue();
                } else if (currentFieldName.equals("max_word_length")) {
                    settings.maxWordLength = parser.intValue();
                } else {
                    throw new ElasticsearchParseException("failed to parse term vectors request. the field [{}] is not valid for filter parameter for term vector request", currentFieldName);
                }
            }
        }
        return settings;
    }
}
