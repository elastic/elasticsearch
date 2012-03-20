/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.highlight;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * A builder for search highlighting.
 *
 * @see org.elasticsearch.search.builder.SearchSourceBuilder#highlight()
 */
public class HighlightBuilder implements ToXContent {

    private List<Field> fields;

    private String tagsSchema;

    private String[] preTags;

    private String[] postTags;

    private String order;

    private String encoder;

    private Boolean requireFieldMatch;

    /**
     * Adds a field to be highlighted with default fragment size of 100 characters, and
     * default number of fragments of 5 using the default encoder
     *
     * @param name The field to highlight
     */
    public HighlightBuilder field(String name) {
        if (fields == null) {
            fields = newArrayList();
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
            fields = newArrayList();
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
            fields = newArrayList();
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
            fields = newArrayList();
        }
        fields.add(new Field(name).fragmentSize(fragmentSize).numOfFragments(numberOfFragments)
                .fragmentOffset(fragmentOffset));
        return this;
    }

    public HighlightBuilder field(Field field) {
        if (fields == null) {
            fields = newArrayList();
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

    public HighlightBuilder requireFieldMatch(boolean requireFieldMatch) {
        this.requireFieldMatch = requireFieldMatch;
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
        if (encoder != null) {
            builder.field("encoder", encoder);
        }
        if (requireFieldMatch != null) {
            builder.field("require_field_match", requireFieldMatch);
        }
        if (fields != null) {
            builder.startObject("fields");
            for (Field field : fields) {
                builder.startObject(field.name());
                if (field.fragmentSize != -1) {
                    builder.field("fragment_size", field.fragmentSize);
                }
                if (field.numOfFragments != -1) {
                    builder.field("number_of_fragments", field.numOfFragments);
                }
                if (field.fragmentOffset != -1) {
                    builder.field("fragment_offset", field.fragmentOffset);
                }
                if (field.requireFieldMatch != null) {
                    builder.field("require_field_match", field.requireFieldMatch);
                }

                builder.endObject();
            }
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    public static class Field {
        final String name;
        int fragmentSize = -1;
        int fragmentOffset = -1;
        int numOfFragments = -1;
        Boolean requireFieldMatch;

        private Field(String name) {
            this.name = name;
        }

        public String name() {
            return name;
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

        public Field requireFieldMatch(boolean requireFieldMatch) {
            this.requireFieldMatch = requireFieldMatch;
            return this;
        }
    }
}
