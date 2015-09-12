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

package org.elasticsearch.search.highlight;

import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A builder for search highlighting. Settings can control how large fields
 * are summarized to show only selected snippets ("fragments") containing search terms.
 *
 * @see org.elasticsearch.search.builder.SearchSourceBuilder#highlight()
 */
public class HighlightBuilder implements ToXContent {

    private List<Field> fields;

    private String tagsSchema;

    private Boolean highlightFilter;

    private Integer fragmentSize;

    private Integer numOfFragments;

    private String[] preTags;

    private String[] postTags;

    private String order;

    private String encoder;

    private Boolean requireFieldMatch;

    private Integer boundaryMaxScan;

    private char[] boundaryChars;

    private String highlighterType;

    private String fragmenter;

    private QueryBuilder highlightQuery;

    private Integer noMatchSize;

    private Integer phraseLimit;

    private Map<String, Object> options;

    private Boolean forceSource;

    private boolean useExplicitFieldOrder = false;

    /**
     * Adds a field to be highlighted with default fragment size of 100 characters, and
     * default number of fragments of 5 using the default encoder
     *
     * @param name The field to highlight
     */
    public HighlightBuilder field(String name) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(new Field(name));
        return this;
    }


    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * default number of fragments of 5.
     *
     * @param name         The field to highlight
     * @param fragmentSize The size of a fragment in characters
     */
    public HighlightBuilder field(String name, int fragmentSize) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(new Field(name).fragmentSize(fragmentSize));
        return this;
    }


    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * a provided (maximum) number of fragments.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     */
    public HighlightBuilder field(String name, int fragmentSize, int numberOfFragments) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(new Field(name).fragmentSize(fragmentSize).numOfFragments(numberOfFragments));
        return this;
    }


    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * a provided (maximum) number of fragments.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     * @param fragmentOffset    The offset from the start of the fragment to the start of the highlight
     */
    public HighlightBuilder field(String name, int fragmentSize, int numberOfFragments, int fragmentOffset) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(new Field(name).fragmentSize(fragmentSize).numOfFragments(numberOfFragments)
                .fragmentOffset(fragmentOffset));
        return this;
    }

    public HighlightBuilder field(Field field) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(field);
        return this;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allows schemes
     * are <tt>styled</tt> and <tt>default</tt>.
     *
     * @param schemaName The tag scheme name
     */
    public HighlightBuilder tagsSchema(String schemaName) {
        this.tagsSchema = schemaName;
        return this;
    }

    /**
     * Set this to true when using the highlighterType <tt>fast-vector-highlighter</tt>
     * and you want to provide highlighting on filter clauses in your
     * query. Default is <tt>false</tt>.
     */
    public HighlightBuilder highlightFilter(boolean highlightFilter) {
        this.highlightFilter = highlightFilter;
        return this;
    }

    /**
     * Sets the size of a fragment in characters (defaults to 100)
     */
    public HighlightBuilder fragmentSize(Integer fragmentSize) {
        this.fragmentSize = fragmentSize;
        return this;
    }

    /**
     * Sets the maximum number of fragments returned
     */
    public HighlightBuilder numOfFragments(Integer numOfFragments) {
        this.numOfFragments = numOfFragments;
        return this;
    }

    /**
     * Set encoder for the highlighting
     * are <tt>styled</tt> and <tt>default</tt>.
     *
     * @param encoder name
     */
    public HighlightBuilder encoder(String encoder) {
        this.encoder = encoder;
        return this;
    }

    /**
     * Explicitly set the pre tags that will be used for highlighting.
     */
    public HighlightBuilder preTags(String... preTags) {
        this.preTags = preTags;
        return this;
    }

    /**
     * Explicitly set the post tags that will be used for highlighting.
     */
    public HighlightBuilder postTags(String... postTags) {
        this.postTags = postTags;
        return this;
    }

    /**
     * The order of fragments per field. By default, ordered by the order in the
     * highlighted text. Can be <tt>score</tt>, which then it will be ordered
     * by score of the fragments.
     */
    public HighlightBuilder order(String order) {
        this.order = order;
        return this;
    }

    /**
     * Set to true to cause a field to be highlighted only if a query matches that field. 
     * Default is false meaning that terms are highlighted on all requested fields regardless 
     * if the query matches specifically on them. 
     */
    public HighlightBuilder requireFieldMatch(boolean requireFieldMatch) {
        this.requireFieldMatch = requireFieldMatch;
        return this;
    }

    /**
     * When using the highlighterType <tt>fast-vector-highlighter</tt> this setting 
     * controls how far to look for boundary characters, and defaults to 20.
     */
    public HighlightBuilder boundaryMaxScan(Integer boundaryMaxScan) {
        this.boundaryMaxScan = boundaryMaxScan;
        return this;
    }

    /**
     * When using the highlighterType <tt>fast-vector-highlighter</tt> this setting 
     * defines what constitutes a boundary for highlighting. It’s a single string with 
     * each boundary character defined in it. It defaults to .,!? \t\n
     */
    public HighlightBuilder boundaryChars(char[] boundaryChars) {
        this.boundaryChars = boundaryChars;
        return this;
    }

    /**
     * Set type of highlighter to use. Supported types
     * are <tt>highlighter</tt>, <tt>fast-vector-highlighter</tt> and <tt>postings-highlighter</tt>.
     * The default option selected is dependent on the mappings defined for your index. 
     * Details of the different highlighter types are covered in the reference guide.
     */
    public HighlightBuilder highlighterType(String highlighterType) {
        this.highlighterType = highlighterType;
        return this;
    }

    /**
     * Sets what fragmenter to use to break up text that is eligible for highlighting.
     * This option is only applicable when using the plain highlighterType <tt>highlighter</tt>.
     * Permitted values are "simple" or "span" relating to {@link SimpleFragmenter} and
     * {@link SimpleSpanFragmenter} implementations respectively with the default being "span"
     */
    public HighlightBuilder fragmenter(String fragmenter) {
        this.fragmenter = fragmenter;
        return this;
    }

    /**
     * Sets a query to be used for highlighting all fields instead of the search query.
     */
    public HighlightBuilder highlightQuery(QueryBuilder highlightQuery) {
        this.highlightQuery = highlightQuery;
        return this;
    }

    /**
     * Sets the size of the fragment to return from the beginning of the field if there are no matches to
     * highlight and the field doesn't also define noMatchSize.
     * @param noMatchSize integer to set or null to leave out of request.  default is null.
     * @return this for chaining
     */
    public HighlightBuilder noMatchSize(Integer noMatchSize) {
        this.noMatchSize = noMatchSize;
        return this;
    }

    /**
     * Sets the maximum number of phrases the fvh will consider if the field doesn't also define phraseLimit.
     * @param phraseLimit maximum number of phrases the fvh will consider
     * @return this for chaining
     */
    public HighlightBuilder phraseLimit(Integer phraseLimit) {
        this.phraseLimit = phraseLimit;
        return this;
    }

    /**
     * Allows to set custom options for custom highlighters.
     */
    public HighlightBuilder options(Map<String, Object> options) {
        this.options = options;
        return this;
    }

    /**
     * Forces the highlighting to highlight fields based on the source even if fields are stored separately.
     */
    public HighlightBuilder forceSource(boolean forceSource) {
        this.forceSource = forceSource;
        return this;
    }

    /**
     * Send the fields to be highlighted using a syntax that is specific about the order in which they should be highlighted.
     * @return this for chaining
     */
    public HighlightBuilder useExplicitFieldOrder(boolean useExplicitFieldOrder) {
        this.useExplicitFieldOrder = useExplicitFieldOrder;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("highlight");
        if (tagsSchema != null) {
            builder.field("tags_schema", tagsSchema);
        }
        if (preTags != null) {
            builder.array("pre_tags", preTags);
        }
        if (postTags != null) {
            builder.array("post_tags", postTags);
        }
        if (order != null) {
            builder.field("order", order);
        }
        if (highlightFilter != null) {
            builder.field("highlight_filter", highlightFilter);
        }
        if (fragmentSize != null) {
            builder.field("fragment_size", fragmentSize);
        }
        if (numOfFragments != null) {
            builder.field("number_of_fragments", numOfFragments);
        }
        if (encoder != null) {
            builder.field("encoder", encoder);
        }
        if (requireFieldMatch != null) {
            builder.field("require_field_match", requireFieldMatch);
        }
        if (boundaryMaxScan != null) {
            builder.field("boundary_max_scan", boundaryMaxScan);
        }
        if (boundaryChars != null) {
            builder.field("boundary_chars", boundaryChars);
        }
        if (highlighterType != null) {
            builder.field("type", highlighterType);
        }
        if (fragmenter != null) {
            builder.field("fragmenter", fragmenter);
        }
        if (highlightQuery != null) {
            builder.field("highlight_query", highlightQuery);
        }
        if (noMatchSize != null) {
            builder.field("no_match_size", noMatchSize);
        }
        if (phraseLimit != null) {
            builder.field("phrase_limit", phraseLimit);
        }
        if (options != null && options.size() > 0) {
            builder.field("options", options);
        }
        if (forceSource != null) {
            builder.field("force_source", forceSource);
        }
        if (fields != null) {
            if (useExplicitFieldOrder) {
                builder.startArray("fields");
            } else {
                builder.startObject("fields");
            }
            for (Field field : fields) {
                if (useExplicitFieldOrder) {
                    builder.startObject();
                }
                builder.startObject(field.name());
                if (field.preTags != null) {
                    builder.field("pre_tags", field.preTags);
                }
                if (field.postTags != null) {
                    builder.field("post_tags", field.postTags);
                }
                if (field.fragmentSize != -1) {
                    builder.field("fragment_size", field.fragmentSize);
                }
                if (field.numOfFragments != -1) {
                    builder.field("number_of_fragments", field.numOfFragments);
                }
                if (field.fragmentOffset != -1) {
                    builder.field("fragment_offset", field.fragmentOffset);
                }
                if (field.highlightFilter != null) {
                    builder.field("highlight_filter", field.highlightFilter);
                }
                if (field.order != null) {
                    builder.field("order", field.order);
                }
                if (field.requireFieldMatch != null) {
                    builder.field("require_field_match", field.requireFieldMatch);
                }
                if (field.boundaryMaxScan != -1) {
                    builder.field("boundary_max_scan", field.boundaryMaxScan);
                }
                if (field.boundaryChars != null) {
                    builder.field("boundary_chars", field.boundaryChars);
                }
                if (field.highlighterType != null) {
                    builder.field("type", field.highlighterType);
                }
                if (field.fragmenter != null) {
                    builder.field("fragmenter", field.fragmenter);
                }
                if (field.highlightQuery != null) {
                    builder.field("highlight_query", field.highlightQuery);
                }
                if (field.noMatchSize != null) {
                    builder.field("no_match_size", field.noMatchSize);
                }
                if (field.matchedFields != null) {
                    builder.field("matched_fields", field.matchedFields);
                }
                if (field.phraseLimit != null) {
                    builder.field("phrase_limit", field.phraseLimit);
                }
                if (field.options != null && field.options.size() > 0) {
                    builder.field("options", field.options);
                }
                if (field.forceSource != null) {
                    builder.field("force_source", field.forceSource);
                }

                builder.endObject();
                if (useExplicitFieldOrder) {
                    builder.endObject();
                }
            }
            if (useExplicitFieldOrder) {
                builder.endArray();
            } else {
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    public static class Field {
        final String name;
        String[] preTags;
        String[] postTags;
        int fragmentSize = -1;
        int fragmentOffset = -1;
        int numOfFragments = -1;
        Boolean highlightFilter;
        String order;
        Boolean requireFieldMatch;
        int boundaryMaxScan = -1;
        char[] boundaryChars;
        String highlighterType;
        String fragmenter;
        QueryBuilder highlightQuery;
        Integer noMatchSize;
        String[] matchedFields;
        Integer phraseLimit;
        Map<String, Object> options;
        Boolean forceSource;

        public Field(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        /**
         * Explicitly set the pre tags for this field that will be used for highlighting.
         * This overrides global settings set by {@link HighlightBuilder#preTags(String...)}.
         */
        public Field preTags(String... preTags) {
            this.preTags = preTags;
            return this;
        }

        /**
         * Explicitly set the post tags for this field that will be used for highlighting.
         * This overrides global settings set by {@link HighlightBuilder#postTags(String...)}.
         */
        public Field postTags(String... postTags) {
            this.postTags = postTags;
            return this;
        }

        public Field fragmentSize(int fragmentSize) {
            this.fragmentSize = fragmentSize;
            return this;
        }

        public Field fragmentOffset(int fragmentOffset) {
            this.fragmentOffset = fragmentOffset;
            return this;
        }

        public Field numOfFragments(int numOfFragments) {
            this.numOfFragments = numOfFragments;
            return this;
        }

        public Field highlightFilter(boolean highlightFilter) {
            this.highlightFilter = highlightFilter;
            return this;
        }

        /**
         * The order of fragments per field. By default, ordered by the order in the
         * highlighted text. Can be <tt>score</tt>, which then it will be ordered
         * by score of the fragments.
         * This overrides global settings set by {@link HighlightBuilder#order(String)}.
         */
        public Field order(String order) {
            this.order = order;
            return this;
        }

        public Field requireFieldMatch(boolean requireFieldMatch) {
            this.requireFieldMatch = requireFieldMatch;
            return this;
        }

        public Field boundaryMaxScan(int boundaryMaxScan) {
            this.boundaryMaxScan = boundaryMaxScan;
            return this;
        }

        public Field boundaryChars(char[] boundaryChars) {
            this.boundaryChars = boundaryChars;
            return this;
        }

        /**
         * Set type of highlighter to use. Supported types
         * are <tt>highlighter</tt>, <tt>fast-vector-highlighter</tt> nad <tt>postings-highlighter</tt>.
         * This overrides global settings set by {@link HighlightBuilder#highlighterType(String)}.
         */
        public Field highlighterType(String highlighterType) {
            this.highlighterType = highlighterType;
            return this;
        }

        /**
         * Sets what fragmenter to use to break up text that is eligible for highlighting.
         * This option is only applicable when using plain / normal highlighter.
         * This overrides global settings set by {@link HighlightBuilder#fragmenter(String)}.
         */
        public Field fragmenter(String fragmenter) {
            this.fragmenter = fragmenter;
            return this;
        }

        /**
         * Sets a query to use for highlighting this field instead of the search query.
         */
        public Field highlightQuery(QueryBuilder highlightQuery) {
            this.highlightQuery = highlightQuery;
            return this;
        }

        /**
         * Sets the size of the fragment to return from the beginning of the field if there are no matches to
         * highlight.
         * @param noMatchSize integer to set or null to leave out of request.  default is null.
         * @return this for chaining
         */
        public Field noMatchSize(Integer noMatchSize) {
            this.noMatchSize = noMatchSize;
            return this;
        }

        /**
         * Allows to set custom options for custom highlighters.
         * This overrides global settings set by {@link HighlightBuilder#options(Map<String, Object>)}.
         */
        public Field options(Map<String, Object> options) {
            this.options = options;
            return this;
        }

        /**
         * Set the matched fields to highlight against this field data.  Default to null, meaning just
         * the named field.  If you provide a list of fields here then don't forget to include name as
         * it is not automatically included.
         */
        public Field matchedFields(String... matchedFields) {
            this.matchedFields = matchedFields;
            return this;
        }

        /**
         * Sets the maximum number of phrases the fvh will consider.
         * @param phraseLimit maximum number of phrases the fvh will consider
         * @return this for chaining
         */
        public Field phraseLimit(Integer phraseLimit) {
            this.phraseLimit = phraseLimit;
            return this;
        }


        /**
         * Forces the highlighting to highlight this field based on the source even if this field is stored separately.
         */
        public Field forceSource(boolean forceSource) {
            this.forceSource = forceSource;
            return this;
        }

    }
}
